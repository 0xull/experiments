use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Ok, Result};
use nix::mount::{MsFlags, mount, umount};

/// Represents an overlay filesystem configuration with required directories.
struct OverlayConfig {
    /// The read-only layer directories, ordered from bottom to top.
    lower_dirs: Vec<PathBuf>,

    /// The single writeable layer.
    upper_dir: PathBuf,

    /// Scratch directory for atomic operations.
    work_dir: PathBuf,

    /// The mount point where merged view appears.
    merged_dir: PathBuf,
}

impl OverlayConfig {
    /// Creates a new overlay configuration with specified directories.
    /// # Arguments
    ///
    /// * `lower_dirs` - Layer directories from bottom to top (topmost layer last)
    /// * `upper_dir` - Writable layer for changes
    /// * `work_dir` - Scratch space (must be empty and on same fs as upper_dir)
    /// * `merged_dir` - Mount point for the unified view
    fn new(
        lower_dirs: Vec<PathBuf>,
        upper_dir: PathBuf,
        work_dir: PathBuf,
        merged_dir: PathBuf,
    ) -> Result<Self> {
        if lower_dirs.is_empty() {
            anyhow::bail!("At least one lower directory is required");
        }

        Ok(Self {
            lower_dirs,
            upper_dir,
            work_dir,
            merged_dir,
        })
    }

    /// Mounts the overlay filesystem, making the merged view available.
    ///
    /// The mount options tells the Linux kernel how configure the overlay with
    /// below string format:
    ///   lowerdir=top:middle:bottom,upperdir=/path,workdir=/path
    fn mount(&self) -> Result<()> {
        // In a real container setup, these directories would already exist.
        // But for demo purposes, create all required directories.
        for dir in &self.lower_dirs {
            fs::create_dir_all(dir)
                .with_context(|| format!("Failed to create lower dir: {:?}", dir))?;
        }
        fs::create_dir_all(&self.upper_dir).context("Failed to create upper directory")?;
        fs::create_dir_all(&self.work_dir).context("Failed to create work directory")?;
        fs::create_dir_all(&self.merged_dir).context("Failed to create merged directory")?;

        // Reverse the internal representation, bottom-to-top, to match
        // the kernel's expected order.
        //
        // Example: if lower_dirs = ["/base", "/app", "/config"],
        // expected kernel order: "lower_dir=/config:/app:/base"
        let lower_str = self
            .lower_dirs
            .iter()
            .rev()
            .map(|p| p.to_string_lossy())
            .collect::<Vec<_>>()
            .join(":");

        let mount_option = format!(
            "lowerdir={},upperdir={},workdir={}",
            lower_str,
            self.upper_dir.to_string_lossy(),
            &self.work_dir.to_string_lossy()
        );

        println!("Mounting overlay with options: {}", mount_option);
        mount(
            Some("overlay"),
            &self.merged_dir,
            Some("overlay"),
            MsFlags::empty(),
            Some(mount_option.as_str()),
        )
        .context("Failed to mount overlay filesystem")?;

        println!("Overlay mounted successfully at {:?}", self.merged_dir);
        Ok(())
    }

    fn umount(&self) -> Result<()> {
        println!("Unmounting overlay at {:?}", self.merged_dir);
        umount(&self.merged_dir).context("Failed to unmount overlay filesystem")?;
        println!("Overlay unmounted successfully");
        Ok(())
    }
}

/// Represents a container image layer stored as a tar archive
struct ImageLayer {
    /// Path to the layer tar archive. In containerd, this would be
    /// /var/lib/containerd/io.containerd.content.v1.content/blobs/sha256/<hash>
    tar_path: PathBuf,

    /// The layer's content hash.
    layer_id: String,
}

impl ImageLayer {
    fn new(tar_path: PathBuf, layer_id: String) -> Self {
        Self { tar_path, layer_id }
    }
}

/// Represents a snapshot in containerd's overlayfs snapshotter.
struct Snapshot {
    /// Unique identifier for this snapshot
    id: String,

    /// The directory containing the extracted layer contents.
    fs_dir: PathBuf,

    /// Parent snapshot IDs that this snapshot builds upon.
    parents: Vec<String>,
}

impl Snapshot {
    fn new(id: String, fs_dir: PathBuf, parents: Vec<String>) -> Self {
        Self {
            id,
            fs_dir,
            parents,
        }
    }
}

/// Simulates containerd's overlayfs snapshotter behaviour. Hence, the focus
/// is on core overlay mechanics.
struct OverlaySnapshotter {
    /// Root directory where all snapshot data is stored.
    /// Corresponds to /var/lib/containerd/io.containerd.snapshotter.v1.overlayfs
    root: PathBuf,

    /// Registry of all snapshots, indexed by IDs.
    snapshots: std::collections::HashMap<String, Snapshot>,
}

impl OverlaySnapshotter {
    /// Creates a new snapshotter instance rooted at specified directory.
    fn new(root: PathBuf) -> Result<Self> {
        fs::create_dir_all(&root)
            .with_context(|| format!("Failed to create snapshotter root: {:?}", root))?;

        Ok(Self {
            root,
            snapshots: std::collections::HashMap::new(),
        })
    }

    /// Unpacks a layer tar archive into a new snapshot. Creates a new snapshot based
    /// on a parent or from scratch for base layers.
    fn unpack_layer(&mut self, layer: &ImageLayer, parent_ids: Vec<String>) -> Result<String> {
        let snapshot_id = layer.layer_id.clone();
        let snapshot_dir = self.root.join("snapshots").join(&snapshot_id);
        let fs_dir = snapshot_dir.join("fs");

        println!("Unpacking layer {} to {:?}", snapshot_id, fs_dir);

        fs::create_dir_all(&fs_dir)
            .with_context(|| format!("Failed to create snapshot fs dir: {:?}", fs_dir))?;

        // Extract the layer tar into the fs directory
        let status = Command::new("tar")
            .arg("-xzf")
            .arg(&layer.tar_path)
            .arg("-C")
            .arg(&fs_dir)
            .status()
            .context("Failed to execute tar command")?;
        if !status.success() {
            anyhow::bail!("Tar extraction failed for layer {}", snapshot_id);
        }

        let snapshot = Snapshot::new(snapshot_id.clone(), fs_dir, parent_ids);
        self.snapshots.insert(snapshot_id.clone(), snapshot);

        println!("Layer {} unpacked successfully", snapshot_id);
        Ok(snapshot_id)
    }

    /// Prepares an overlay mount for a container from a stack of image layer snapshots.
    /// Creates a merged view where the container can read all image layers, while writes
    /// go to its private upper directory.
    fn prepare_container(
        &mut self,
        container_id: &str,
        image_snapshot_ids: Vec<String>,
    ) -> Result<OverlayConfig> {
        println!("\nPreparing container filesystem: {}", container_id);

        for snapshot_id in &image_snapshot_ids {
            if !self.snapshots.contains_key(snapshot_id) {
                anyhow::bail!("Image snapshot not found: {}", snapshot_id);
            }
        }

        let container_snapshot_dir = self.root.join("snapshots").join(container_id);
        let upper_dir = container_snapshot_dir.join("fs");
        let work_dir = container_snapshot_dir.join("work");
        let merged_dir = container_snapshot_dir.join("merged");
        
        fs::create_dir_all(&upper_dir)
            .context("Failed to create container upper dir")?;
        fs::create_dir_all(&work_dir)
            .context("Failed to create container work dir")?;
        fs::create_dir_all(&merged_dir)
            .context("Failed to create container merged dir")?;

        let lower_dirs: Vec<PathBuf> = image_snapshot_ids
            .iter()
            .map(|id| {
                self.snapshots
                    .get(id)
                    .expect("Snapshot should exist (validated above)")
                    .fs_dir
                    .clone()
            })
            .collect();

        println!(
            "Container will use {} image layers as lower dirs",
            lower_dirs.len()
        );
        for (i, dir) in lower_dirs.iter().enumerate() {
            println!("  Layer {}: {:?}", i, dir);
        }

        let container_snapshot = Snapshot::new(
            container_id.to_string(),
            upper_dir.clone(),
            image_snapshot_ids,
        );
        self.snapshots
            .insert(container_id.to_string(), container_snapshot);

        let config = OverlayConfig::new(lower_dirs, upper_dir, work_dir, merged_dir)?;

        Ok(config)
    }
}

/// Demos overlay filesystem behaviour, creating layers with conflicting files
/// and shows overlayfs resolution of the conflicts.
fn demonstrate_overlay() -> Result<()> {
    println!("\n=== OverlayFS Demo ===\n");

    let base = Path::new("/tmp/overlay-demo");
    let _ = fs::remove_dir_all(base);

    let lower1 = base.join("lower1");
    let lower2 = base.join("lower2");
    let lower3 = base.join("lower3");
    let upper = base.join("upper");
    let work = base.join("work");
    let merged = base.join("merged");

    // Populate bottom layer
    println!("Creating lower1 (base layer)...");
    fs::create_dir_all(&lower1)?;
    fs::write(lower1.join("base.txt"), "I am from the base layer\n")?;
    fs::write(lower1.join("shared.txt"), "Version from base layer\n")?;

    // Populate middle layer
    println!("Creating lower2 (middle layer)...");
    fs::create_dir_all(&lower2)?;
    fs::write(lower2.join("app.txt"), "I am from the app layer\n")?;
    fs::write(lower2.join("shared.txt"), "Version from app layer\n")?;

    // Populate top layer
    println!("Creating lower3 (top layer)...");
    fs::create_dir_all(&lower3)?;
    fs::write(lower3.join("config.txt"), "I am from the config layer\n")?;
    fs::write(lower3.join("shared.txt"), "Version from config layer\n")?;

    let config = OverlayConfig::new(
        vec![lower1.clone(), lower2.clone(), lower3.clone()],
        upper.clone(),
        work.clone(),
        merged.clone(),
    )?;
    config.mount()?;

    println!("\n=== Examining merged view ===\n");

    let shared_content = fs::read_to_string(merged.join("shared.txt"))?;
    assert_eq!(shared_content, "Version from config layer\n");

    println!("Files in merged view:");
    for entry in fs::read_dir(&merged)? {
        let entry = entry?;
        println!("  - {}", entry.file_name().to_string_lossy());
    }

    println!("\n=== Demoing copy-on-write ===\n");

    fs::write(merged.join("base.txt"), "Modified in upper layer!\n")?;
    let modified_content = fs::read_to_string(merged.join("base.txt"))?;
    let upper_content = fs::read_to_string(upper.join("base.txt"))?;
    assert_eq!(modified_content, upper_content);

    let lower_content = fs::read_to_string(lower1.join("base.txt"))?;
    assert_eq!(lower_content, "I am from the base layer\n");

    println!("\n=== Demoing file creation ===\n");

    fs::write(merged.join("newfile.txt"), "I am a new file\n")?;
    assert!(merged.join("newfile.txt").exists());
    assert!(upper.join("newfile.txt").exists());
    assert!(!lower1.join("newfile.txt").exists());
    assert!(!lower2.join("newfile.txt").exists());
    assert!(!lower3.join("newfile.txt").exists());
    println!("newfile.txt successfully created in upper directory");

    println!("\n=== Demoing deletion (whiteout) ===\n");

    fs::remove_file(merged.join("config.txt"))?;
    assert!(!merged.join("config.txt").exists());
    assert!(lower3.join("config.txt").exists());

    let whiteout_path = upper.join("config.txt");
    if whiteout_path.exists() {
        println!(
            "Whiteout marker created in upper directory at: {:?}",
            whiteout_path
        );
        let metadata = fs::metadata(whiteout_path)?;
        let file_type = metadata.file_type();
        println!("Whiteout file type: {:?}", file_type);
        // On Linux, this would be a character device, but Rust's standard metadata
        // doesn't expose device major/minor numbers directly.
    }

    println!("\n=== Cleanup ===\n");

    config.umount()?;

    println!("Upper directory contents after unmount:");
    for entry in fs::read_dir(upper)? {
        let entry = entry?;
        println!("  - {}", entry.file_name().to_string_lossy());
    }

    fs::remove_dir_all(base)?;

    Ok(())
}

fn demonstrate_containerd_workflow() -> Result<()> {
    println!("\n=== Simulating  Containerd's Overlayfs Snapshotter Workflow ===\n");

    let base = Path::new("/tmp/containerd-demo");
    let _ = fs::remove_dir_all(base);
    fs::create_dir_all(base)?;

    println!("=== Creating image layer tarballs ===\n");

    // Layer 1: Base OS layer (simulating ubuntu:22.04 base)
    let layer1_dir = base.join("layer1-contents");
    fs::create_dir_all(&layer1_dir)?;
    fs::create_dir_all(layer1_dir.join("etc"))?;
    fs::create_dir_all(layer1_dir.join("usr/bin"))?;
    fs::write(layer1_dir.join("etc/os-release"), "Ubuntu 22.04 LTS\n")?;
    fs::write(layer1_dir.join("usr/bin/sh"), "#!/bin/sh\n# Shell binary\n")?;

    let layer1_tar = base.join("layer1.tar.gz");
    create_tarball(&layer1_dir, &layer1_tar)?;
    println!("Created base layer: {:?}", layer1_tar);

    // Layer 2: Application layer (simulating nginx installation)
    let layer2_dir = base.join("layer2-contents");
    fs::create_dir_all(&layer2_dir)?;
    fs::create_dir_all(layer2_dir.join("usr/sbin"))?;
    fs::create_dir_all(layer2_dir.join("etc/nginx"))?;
    fs::write(
        layer2_dir.join("usr/sbin/nginx"),
        "#!/bin/sh\n# Nginx binary\n",
    )?;
    fs::write(
        layer2_dir.join("etc/nginx/nginx.conf"),
        "# Default nginx configurations\n",
    )?;

    let layer2_tar = base.join("layer2.tar.gz");
    create_tarball(&layer2_dir, &layer2_tar)?;
    println!("Created app layer: {:?}", layer2_tar);

    // Layer 3: Configuration layer (simulating a COPY of custom config)
    let layer3_dir = base.join("layer3-contents");
    fs::create_dir_all(&layer3_dir)?;
    fs::create_dir_all(layer3_dir.join("etc/nginx"))?;
    fs::write(
        layer3_dir.join("etc/nginx/nginx.conf"),
        "# Custom nginx configuration\nserver { listen 80; }\n",
    )?;

    let layer3_tar = base.join("layer3.tar.gz");
    create_tarball(&layer3_dir, &layer3_tar)?;
    println!("Created config layer: {:?}\n", layer3_tar);

    println!("=== Unpacking layers into snapshots ===\n");

    let snapshotter_root = base.join("snapshotter");
    let mut snapshotter = OverlaySnapshotter::new(snapshotter_root)?;

    let layer1 = ImageLayer::new(layer1_tar, "sha256-layer1".to_string());
    let snapshot1_id = snapshotter.unpack_layer(&layer1, vec![])?;

    let layer2 = ImageLayer::new(layer2_tar, "sha256-layer2".to_string());
    let snapshot2_id = snapshotter.unpack_layer(&layer2, vec![snapshot1_id.clone()])?;

    let layer3 = ImageLayer::new(layer3_tar, "sha-layer3".to_string());
    let snapshot3_id = snapshotter.unpack_layer(&layer3, vec![snapshot2_id.clone()])?;

    println!(
        "\nAll layers unpacked into snapshot chain: {} -> {} -> {}\n",
        snapshot1_id, snapshot2_id, snapshot3_id
    );

    println!("=== Preparing container filesystem ===\n");

    let container_id = "container-nginx-001";
    let overlay_config = snapshotter.prepare_container(
        container_id,
        vec![
            snapshot1_id.clone(),
            snapshot2_id.clone(),
            snapshot3_id.clone(),
        ],
    )?;

    println!("=== Mounting container overlay ===\n");
    overlay_config.mount()?;

    println!("=== Examining container filesystem ===\n");

    let merged = &overlay_config.merged_dir;
    println!("Files visible in container filesystem:");
    let os_release = fs::read_to_string(merged.join("etc/os-release"))?;
    println!("  /etc/os-release (from layer 1): {}", os_release.trim());
    assert!(merged.join("usr/sbin/nginx").exists());
    println!("  /etc/sbin/nginx exists (from layer 2)");
    let nginx_conf = fs::read_to_string(merged.join("etc/nginx/nginx.conf"))?;
    println!(
        "  /etc/nginx/nginx.conf (from layer 3 - overrode layer 2):\n{}",
        nginx_conf
    );

    println!("\nContainer modifying /etc/os-release...");
    fs::write(
        merged.join("etc/os-release"),
        "Ubuntu 22.04 LTS\nMODIFIED BY CONTAINER",
    )?;

    println!("Container creating /var/log/nginx/app.log...");
    fs::create_dir_all(merged.join("var/log/nginx"))?;
    fs::write(
        merged.join("var/log/nginx/app.log"),
        "Container application log",
    )?;

    // Check writeable layer for modifications
    let upper = &overlay_config.upper_dir;

    println!("\nChecking upper directory (container's private layer):");
    assert!(upper.join("etc/os-release").exists());
    println!("  /etc/os-release exists (copied up from layer 1, then modified)");

    assert!(upper.join("var/log/nginx/app.log").exists());
    println!("  /var/log/nginx/app.log exists (new file created by container)");

    let layer1_snapshot = snapshotter.snapshots.get(&snapshot1_id).unwrap();
    let original_os_release = fs::read_to_string(layer1_snapshot.fs_dir.join("etc/os-release"))?;
    println!("\nOriginal /etc/os-release in layer 1 snapshot:");
    println!("{}", original_os_release);
    assert_eq!(original_os_release, "Ubuntu 22.04 LTS\n");

    println!("\n=== Demoing layer sharing ===\n");

    println!("Creating second container from the same image...");
    let container2_id = "container-nginx-002";
    let overlay_config2 = snapshotter.prepare_container(
        container2_id,
        vec![
            snapshot1_id.clone(),
            snapshot2_id.clone(),
            snapshot3_id.clone(),
        ],
    )?;

    overlay_config2.mount()?;
    println!("Second container mounted successfully");

    println!("\nBoth containers share same read-only image layers. But separate private layers:");
    println!("  Container 1 upper: {:?}", overlay_config.upper_dir);
    println!("  Container 2 upper: {:?}", overlay_config2.upper_dir);

    // Container 2's modification get its own copy in private writeable layer.
    fs::write(
        overlay_config2.merged_dir.join("etc/os-release"),
        "Ubuntu 22.04 LTS\nMODIFIED BY CONTAINER 2",
    )?;
    assert!(overlay_config2.upper_dir.join("etc/os-release").exists());
    
    // Cleanup
    println!("\n=== Cleanup ===\n");
    overlay_config.umount()?;
    overlay_config2.umount()?;
    
    Command::new("sync").status().context("Failed to sync")?;
    fs::remove_dir_all(base)?;
    
    println!("Demoing completed!\n");
    Ok(())
}

/// Helper function to create a tarball from a directory.
fn create_tarball(source_dir: &Path, output_tar: &Path) -> Result<()> {
    let status = Command::new("tar")
        .arg("-czf")
        .arg(output_tar)
        .arg("-C")
        .arg(source_dir)
        .arg(".")
        .status()
        .context("Failed to execute tar command")?;
    if !status.success() {
        anyhow::bail!("Failed to create tarball: {:?}", output_tar);
    }

    Ok(())
}

fn main() -> Result<()> {
    // demos
    // demonstrate_overlay()?;
    demonstrate_containerd_workflow()?;
    Ok(())
}
