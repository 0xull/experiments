use anyhow::{Context, Result};
use std::{
    path::{Path, PathBuf},
    process::Command,
};

/// Represents a loop device, presenting a file as a block device.
struct LoopDevice {
    /// Path to the loop device in /dev
    device_path: PathBuf,

    /// Path to the backing file
    backing_file: PathBuf,

    /// Whether this loop device should be detached when dropped
    should_cleanup: bool,
}

impl LoopDevice {
    /// Create a backing sparse file and attaches it to a loop device.
    fn create(backing_file: PathBuf, size_mb: u64) -> Result<Self> {
        println!("Creating backing file: {:?} ({}MB)", backing_file, size_mb);

        let size_bytes = size_mb * 1024 * 1024;

        // Create and allocate file
        std::fs::File::create(&backing_file)
            .with_context(|| format!("Failed to create backing file: {:?}", backing_file))?;

        let status = Command::new("fallocate")
            .arg("-l")
            .arg(format!("{}", size_bytes))
            .arg(&backing_file)
            .status()
            .context("Failed to run fallocate")?;

        if !status.success() {
            anyhow::bail!("fallocate failed");
        }

        println!("Backing file allocated");

        // Find a free loop device
        let output = Command::new("losetup")
            .arg("-f") // Just find, don't attach yet
            .output()
            .context("Failed to find free loop device")?;

        if !output.status.success() {
            anyhow::bail!("Failed to find free loop device");
        }

        let device_path = String::from_utf8(output.stdout)
            .context("losetup output is not valid UTF-8")?
            .trim()
            .to_string();

        println!("Found free loop device: {}", device_path);

        // Now explicitly attach the file to that device
        let status = Command::new("losetup")
            .arg(&device_path)
            .arg(&backing_file)
            .status()
            .context("Failed to attach loop device")?;

        if !status.success() {
            anyhow::bail!("Failed to attach loop device");
        }

        println!("Loop device attached: {}", device_path);

        // Verify the size
        let size_check = Command::new("blockdev")
            .arg("--getsize64")
            .arg(&device_path)
            .output()?;

        let reported_size = String::from_utf8(size_check.stdout)?
            .trim()
            .parse::<u64>()?;

        println!("Loop device size: {} bytes", reported_size);

        if reported_size != size_bytes {
            anyhow::bail!(
                "Loop device has wrong size: {} (expected {})",
                reported_size,
                size_bytes
            );
        }

        Ok(Self {
            device_path: PathBuf::from(device_path),
            backing_file,
            should_cleanup: true,
        })
    }

    fn device_path(&self) -> &Path {
        &self.device_path
    }

    // Detaches the loop device from its backing file.
    fn detach(&mut self) -> Result<()> {
        if !self.should_cleanup {
            return Ok(());
        }

        let status = Command::new("losetup")
            .arg("-d")
            .arg(&self.device_path)
            .status()
            .context("Failed to execute losetup -d")?;
        if !status.success() {
            anyhow::bail!("Failed to detach loop device: {:?}", self.device_path);
        }

        self.should_cleanup = false;
        Ok(())
    }
}

impl Drop for LoopDevice {
    fn drop(&mut self) {
        if self.should_cleanup {
            let _ = self.detach();
        }
    }
}

/// Represents a device mapper thin provisioning pool.
struct ThinPool {
    /// Name of the thin pool (/dev/mapper/<name>)
    name: String,

    /// Loop device providing storage for metadata
    metadata_dev: LoopDevice,

    /// Loop device providing storage for data
    data_dev: LoopDevice,

    /// Size of data chunks in the pool (in 512-byte sectors)
    data_block_size: u64,

    /// Whether this pool should be cleaned up when dropped
    should_cleanup: bool,
}

impl ThinPool {
    /// Create a new thin pool with the specified configuration.
    fn create(
        pool_name: String,
        metadata_size_mb: u64,
        data_size_mb: u64,
        data_block_size_sectors: u64,
    ) -> Result<Self> {
        let metadata_backing = PathBuf::from(format!("/tmp/{}-metadata.img", pool_name));
        let data_backing = PathBuf::from(format!("/tmp/{}-data.img", pool_name));

        let _ = std::fs::remove_file(&metadata_backing);
        let _ = std::fs::remove_file(&data_backing);

        let metadata_dev = LoopDevice::create(metadata_backing, metadata_size_mb)
            .context("Failed to create metadata loop device")?;
        let data_dev = LoopDevice::create(data_backing, data_size_mb)
            .context("Failed to create data loop device")?;
        println!("\nLoop devices created:");
        println!("  Metadata: {:?}", metadata_dev.device_path());
        println!("  Data: {:?}", data_dev.device_path());

        // The metadata device needs to have a valid thin pool structure before
        // creating the pool device
        Self::initialize_metadata(
            metadata_dev.device_path(),
            data_size_mb,
            data_block_size_sectors,
        )?;

        Self::create_pool_device(
            &pool_name,
            metadata_dev.device_path(),
            data_dev.device_path(),
            data_block_size_sectors,
        )?;

        Ok(Self {
            name: pool_name,
            metadata_dev,
            data_dev,
            data_block_size: data_block_size_sectors,
            should_cleanup: true,
        })
    }

    //// Initialize a metadata device with an empty thin pool structure, storing
    /// mapping tables and allocation bitmaps for the thin pool.
    fn initialize_metadata(
        metadata_path: &Path,
        data_size_mb: u64,
        data_block_size_sectors: u64,
    ) -> Result<()> {
        let data_size_sectors = data_size_mb * 1024 * 1024 / 512;
        let nr_data_blocks = data_size_sectors / data_block_size_sectors;
        
        // Create XML description of an empty thin pool; thin-provisioning-tools use
        // the XML format to represent the metadata structure.
        let metadata_xml = format!(
            r#"<superblock uuid="" time="0" transaction="0" data_block_size="{}" nr_data_blocks="{}"></superblock>"#,
            data_block_size_sectors, nr_data_blocks
        );
        let xml_file = PathBuf::from("/tmp/thin-metadata-init.xml");
        std::fs::write(&xml_file, metadata_xml)
            .context("Failed to write metadata XML to file")?;
            
        let status = Command::new("thin_restore")
            .arg("-i")
            .arg(&xml_file)
            .arg("-o")
            .arg(metadata_path)
            .status()
            .context("Failed to run thin_restore")?;
    
        let _ = std::fs::remove_file(&xml_file);
    
        if !status.success() {
            anyhow::bail!("thin_restore failed with status: {}", status);
        }
    
        Ok(())
    }

    /// Creates device mapper thin pool device.
    fn create_pool_device(
        pool_name: &str,
        metadata_path: &Path,
        data_path: &Path,
        data_block_size_sectors: u64,
    ) -> Result<()> {
        let data_size_sectors = Self::get_device_size_sectors(data_path)?;
        // thin-pool table format
        let table = format!(
            "0 {} thin-pool {} {} {} 0 1 skip_block_zeroing",
            data_size_sectors,
            metadata_path.display(),
            data_path.display(),
            data_block_size_sectors,
        );

        let status = Command::new("dmsetup")
            .arg("create")
            .arg(pool_name)
            .arg("--table")
            .arg(&table)
            .arg("--verifyudev")
            .status()
            .context("Failed to execute dmsetup create")?;
        if !status.success() {
            anyhow::bail!("dmsetup create failed for pool: {}", pool_name);
        }

        Ok(())
    }

    /// Returns the size of a block device in 512-byte sectors.
    fn get_device_size_sectors(device_path: &Path) -> Result<u64> {
        let output = Command::new("blockdev")
            .arg("--getsz")
            .arg(device_path)
            .output()
            .context("Failed to run blockdev --getsz")?;

        if !output.status.success() {
            anyhow::bail!("blockdev --getsz failed");
        }

        let size_str =
            String::from_utf8(output.stdout).context("blockdev output is not a valid UTF-8")?;
        let size = size_str
            .trim()
            .parse::<u64>()
            .context("Failed to parse device size")?;
        Ok(size)
    }

    /// Creates a thin volume from this pool.
    fn create_thin_volume(
        &self,
        volume_name: &str,
        virtual_size_mb: u64,
        device_id: u32,
    ) -> Result<ThinVolume> {
        let message = format!("create_thin {}", device_id);
        let status = Command::new("dmsetup")
            .arg("message")
            .arg(&format!("/dev/mapper/{}", self.name))
            .arg("0") // Message to sector 0
            .arg(&message)
            .status()
            .context("Failed to create_thin message")?;
        if !status.success() {
            anyhow::bail!("Failed to create thin volume with ID {}", device_id);
        }

        // thin volume table format
        let virtual_size_sectors = virtual_size_mb * 1024 * 1024 / 512;
        let table = format!(
            "0 {} thin /dev/mapper/{} {}",
            virtual_size_sectors, self.name, device_id
        );
        let status = Command::new("dmsetup")
            .arg("create")
            .arg(volume_name)
            .arg("--table")
            .arg(&table)
            .arg("--verifyudev")
            .status()
            .context("Failed to create thin volume device")?;
        if !status.success() {
            anyhow::bail!("dmsetup create failed for volume: {}", volume_name);
        }

        Ok(ThinVolume {
            name: volume_name.to_string(),
            pool_name: self.name.clone(),
            device_id,
            virtual_size_mb,
            should_cleanup: true,
        })
    }

    /// Removes the thin pool device and cleans up resources.
    fn remove(&mut self) -> Result<()> {
        if !self.should_cleanup {
            return Ok(());
        }

        let status = Command::new("dmsetup")
            .arg("remove")
            .arg(&self.name)
            .status()
            .context("Failed to remove thin pool")?;
        if !status.success() {
            anyhow::bail!("Failed to remove pool: {}", self.name);
        }

        self.should_cleanup = false;
        Ok(())
    }
}

impl Drop for ThinPool {
    fn drop(&mut self) {
        let _ = self.remove();
    }
}

/// Represents a thin volume created from a thin pool.
struct ThinVolume {
    /// Name of the volume (/dev/mapper/<name>)
    name: String,

    /// Name of the parent pool
    pool_name: String,

    /// Unique device ID within the pool
    device_id: u32,

    /// Virutal size in megabytes
    virtual_size_mb: u64,

    /// Whether this volume should be cleaned up when dropped
    should_cleanup: bool,
}

impl ThinVolume {
    /// Returns the path to the volume's block device
    fn device_path(&self) -> PathBuf {
        PathBuf::from(format!("/dev/mapper/{}", self.name))
    }

    /// Creates a snapshot of this thin volume.
    fn create_snapshot(&self, snapshot_name: &str, snapshot_device_id: u32) -> Result<ThinVolume> {
        let message = format!("create_snap {} {}", snapshot_device_id, self.device_id);
        let status = Command::new("dmsetup")
            .arg("message")
            .arg(&format!("/dev/mapper/{}", self.pool_name))
            .arg("0")
            .arg(&message)
            .status()
            .context("Failed to send create_snap message")?;
        if !status.success() {
            anyhow::bail!("Failed to create snapshot with ID {}", snapshot_device_id)
        }

        let virtual_size_sectors = self.virtual_size_mb * 1024 * 1024 / 512;
        let table = format!(
            "0 {} thin /dev/mapper/{} {}",
            virtual_size_sectors, self.pool_name, snapshot_device_id,
        );
        let status = Command::new("dmsetup")
            .arg("create")
            .arg(snapshot_name)
            .arg("--table")
            .arg(&table)
            .arg("--verifyudev")
            .status()
            .context("Failed to create snapshot device")?;
        if !status.success() {
            anyhow::bail!("dmsetup create failed for snapshot: {}", snapshot_name);
        }

        Ok(ThinVolume {
            name: snapshot_name.to_string(),
            pool_name: self.pool_name.clone(),
            device_id: snapshot_device_id,
            virtual_size_mb: self.virtual_size_mb,
            should_cleanup: true,
        })
    }

    /// Removes the thin volume.
    fn remove(&mut self) -> Result<()> {
        if !self.should_cleanup {
            return Ok(());
        }

        let status = Command::new("dmsetup")
            .arg("remove")
            .arg(&self.name)
            .status()
            .context("Failed to remove thin volume device")?;
        if !status.success() {
            anyhow::bail!("Failed to remove volume: {}", self.name);
        }

        let message = format!("delete {}", self.device_id);
        let status = Command::new("dmsetup")
            .arg("message")
            .arg(&format!("/dev/mapper/{}", self.pool_name))
            .arg("0")
            .arg(&message)
            .status()
            .context("Failed to send delete message")?;
        if !status.success() {
            eprintln!(
                "Warning: Failed to delete thin device {} from pool",
                self.device_id
            );
        }

        self.should_cleanup = false;
        Ok(())
    }
}

impl Drop for ThinVolume {
    fn drop(&mut self) {
        let _ = self.remove();
    }
}

/// Demo thin provisioning with snapshots
fn demonstrate_thin_provisioning() -> Result<()> {
    println!("\n=== Thin Provisioning Demo ===\n");
    let pool = ThinPool::create(
        "demo-pool".to_string(),
        100,  // 100MB metadata
        1024, // 1GB data
        2048, // 1MB chunks (2048 sectors of 512 bytes each)
    )?;
    println!("\n--- Creating base volume ---");
    let base_volume = pool.create_thin_volume("demo-base", 500, 0)?;

    println!("\nFormatting base volume with ext4...");
    let status = Command::new("mkfs.ext4")
        .arg("-q")
        .arg(base_volume.device_path())
        .status()
        .context("Failed to format base volume")?;
    if !status.success() {
        anyhow::bail!("mkfs.ext4 failed");
    }
    println!("Base volume formatted successfully");

    let mount_point = PathBuf::from("/tmp/thin-demo-mount");
    std::fs::create_dir_all(&mount_point)?;

    println!("\nMounting base volume and writing data...");
    nix::mount::mount(
        Some(base_volume.device_path().as_path()),
        &mount_point,
        Some("ext4"),
        nix::mount::MsFlags::empty(),
        None::<&str>,
    )?;

    std::fs::write(
        mount_point.join("base-file.txt"),
        "This is data from the base layer\n",
    )?;
    std::fs::write(
        mount_point.join("shared-file.txt"),
        "Original version of shared file\n",
    )?;
    println!("Files written to base volume:");
    println!("  - base-file.txt");
    println!("  - shared-file.txt");

    nix::mount::umount(&mount_point)?;

    println!("\n--- Creating snapshot (like an image layer) ---");
    let snapshot = base_volume.create_snapshot("demo-snapshot", 1)?;

    println!("\nMounting snapshot and modifying data...");
    nix::mount::mount(
        Some(snapshot.device_path().as_path()),
        &mount_point,
        Some("ext4"),
        nix::mount::MsFlags::empty(),
        None::<&str>,
    )?;

    std::fs::write(
        mount_point.join("shared-file.txt"),
        "Modified version in snapshot\n",
    )?;
    std::fs::write(
        mount_point.join("snapshot-file.txt"),
        "This file only exists in snapshot\n",
    )?;
    println!("Snapshot modified:");
    println!("  - shared-file.txt (modified - triggers copy-on-write)");
    println!("  - snapshot-file.txt (new file)");

    nix::mount::umount(&mount_point)?;

    println!("\n--- Verifying base volume is unchanged ---");
    nix::mount::mount(
        Some(base_volume.device_path().as_path()),
        &mount_point,
        Some("ext4"),
        nix::mount::MsFlags::empty(),
        None::<&str>,
    )?;

    let shared_content = std::fs::read_to_string(mount_point.join("shared-file.txt"))?;
    println!("Content of shared-file.txt in base volume:");
    println!("  {}", shared_content.trim());
    assert_eq!(shared_content, "Original version of shared file\n");

    let snapshot_file_exists = mount_point.join("snapshot-file.txt").exists();
    println!(
        "Does snapshot-file.txt exist in base? {}",
        snapshot_file_exists
    );
    assert!(!snapshot_file_exists);

    println!("\nConfirmed: Base volume unchanged despite snapshot modifications");
    println!("This demonstrates copy-on-write - the snapshot has its own copy of modified blocks");

    nix::mount::umount(&mount_point)?;
    std::fs::remove_dir(&mount_point)?;

    println!("\n--- Pool Status ---");
    let output = Command::new("dmsetup")
        .arg("status")
        .arg("demo-pool")
        .output()?;
    println!("{}", String::from_utf8_lossy(&output.stdout));

    println!("Thin provisioning demonstration complete\n");
    Ok(())
}

/// Demo creating and using a loop device.
fn demonstrate_loop_device() -> Result<()> {
    println!("\n=== Loop Device Demo ===\n");

    let backing_file = PathBuf::from("/tmp/loop-demo-backing.img");
    let _ = std::fs::remove_file(&backing_file);
    let loop_dev = LoopDevice::create(backing_file.clone(), 100)?;

    println!("\nLoop device created successfully!");
    println!("Backing file: {:?}", backing_file);
    println!("Loop device: {:?}", loop_dev.device_path());

    println!("\nVerifying loop device with lsblk...");
    let output = Command::new("lsblk")
        .arg(loop_dev.device_path())
        .output()
        .context("Failed to run lsblk")?;
    println!("{}", String::from_utf8_lossy(&output.stdout));

    println!("Getting detailed info with losetup --list...");
    let output = Command::new("losetup")
        .arg("--list")
        .arg("--output")
        .arg("NAME,SIZELIMIT,OFFSET,AUTOCLEAR,RO,BACK-FILE")
        .output()
        .context("Failed to run losetup --list")?;
    println!("{}", String::from_utf8_lossy(&output.stdout));

    println!("\n=== Cleanup ===\n");

    drop(loop_dev);
    std::fs::remove_file(&backing_file).context("Failed to remove backing file")?;

    Ok(())
}

fn main() -> Result<()> {
    // demonstrate_loop_device()?;
    demonstrate_thin_provisioning()?;
    Ok(())
}
