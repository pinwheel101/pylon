"""
Animation Asset Intelligence Platform — Frame Extraction Pipeline

Step 1 of the indexing pipeline:
  MP4 → Scene Detection → Keyframe Extraction → Structured Storage + Metadata

Usage:
  python -m src.pipeline.extract_frames /path/to/episode.mp4
  python -m src.pipeline.extract_frames /path/to/episode.mp4 --season 1 --episode 1 --title "Simpsons Roasting"
"""
import av
import argparse
import numpy as np
import time
from pathlib import Path
from typing import Optional
from dataclasses import dataclass
from PIL import Image

# Scene detection
from scenedetect import open_video, SceneManager
from scenedetect.detectors import ContentDetector

# Progress display
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

# Project imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from configs.settings import config
from src.db.database import Database

console = Console()


@dataclass
class SceneInfo:
    """Detected scene boundaries."""
    scene_number: int
    start_frame: int
    end_frame: int
    start_time_sec: float
    end_time_sec: float
    duration_sec: float


@dataclass
class FrameInfo:
    """Extracted keyframe metadata."""
    frame_number: int
    timestamp_sec: float
    timestamp_str: str
    scene_number: int
    is_scene_start: bool
    is_scene_end: bool
    is_scene_middle: bool
    image_path: str
    thumbnail_path: str
    width: int
    height: int
    file_size_kb: float


def format_timestamp(seconds: float) -> str:
    """Convert seconds to MM:SS.ms string."""
    minutes = int(seconds // 60)
    secs = seconds % 60
    return f"{minutes:02d}:{secs:05.2f}"


def get_video_info(video_path: Path) -> dict:
    """Extract basic video metadata using PyAV."""
    container = av.open(str(video_path))
    stream = container.streams.video[0]

    fps = float(stream.average_rate) if stream.average_rate else 0.0
    total_frames = stream.frames if stream.frames > 0 else int(fps * float(stream.duration * stream.time_base)) if stream.duration else 0
    duration_sec = float(stream.duration * stream.time_base) if stream.duration else 0.0

    info = {
        "fps": fps,
        "total_frames": total_frames,
        "width": stream.codec_context.width,
        "height": stream.codec_context.height,
        "duration_sec": duration_sec,
        "file_size_mb": video_path.stat().st_size / (1024 * 1024),
    }
    container.close()
    return info


def detect_scenes(video_path: Path, threshold: float = None, min_scene_length: float = None) -> list[SceneInfo]:
    """
    Detect scene transitions using PySceneDetect ContentDetector.

    Returns list of SceneInfo with frame numbers and timestamps.
    """
    threshold = threshold or config.extraction.scene_threshold
    min_scene_length = min_scene_length or config.extraction.min_scene_length_sec

    console.print(f"\n[bold cyan]Scene Detection[/] — threshold={threshold}, min_length={min_scene_length}s")

    video = open_video(str(video_path))
    scene_manager = SceneManager()
    scene_manager.add_detector(ContentDetector(threshold=threshold, min_scene_len=15))

    # Process video
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Detecting scenes...", total=None)
        scene_manager.detect_scenes(video, show_progress=False)
        progress.update(task, completed=True)

    scene_list = scene_manager.get_scene_list()
    fps = video.frame_rate

    scenes = []
    for i, (start, end) in enumerate(scene_list):
        start_sec = start.get_seconds()
        end_sec = end.get_seconds()
        duration = end_sec - start_sec

        # Filter out very short scenes (likely transitions)
        if duration < min_scene_length:
            continue

        scenes.append(SceneInfo(
            scene_number=len(scenes) + 1,
            start_frame=start.get_frames(),
            end_frame=end.get_frames(),
            start_time_sec=start_sec,
            end_time_sec=end_sec,
            duration_sec=duration,
        ))

    console.print(f"  → Detected [bold green]{len(scenes)}[/] scenes (from {len(scene_list)} raw cuts)")
    return scenes


def select_keyframes(scenes: list[SceneInfo], fps: float, strategy: str = None) -> list[dict]:
    """
    Select keyframes from each scene based on strategy.

    Strategies:
      - "first_middle_last": 3 frames per scene (start, middle, end)
      - "first_last": 2 frames per scene
      - "first": 1 frame per scene (fastest, minimum)

    Returns list of dicts with frame_number and metadata.
    """
    strategy = strategy or config.extraction.keyframe_strategy
    keyframes = []
    seen_frames = set()

    for scene in scenes:
        candidates = []
        start = scene.start_frame
        end = max(scene.end_frame - 1, start)  # avoid overshoot
        middle = (start + end) // 2

        if strategy == "first_middle_last":
            candidates = [
                (start, True, False, False),   # (frame, is_start, is_end, is_middle)
                (middle, False, False, True),
                (end, False, True, False),
            ]
        elif strategy == "first_last":
            candidates = [
                (start, True, False, False),
                (end, False, True, False),
            ]
        elif strategy == "first":
            candidates = [(start, True, False, False)]
        else:
            # Default: first_middle_last
            candidates = [
                (start, True, False, False),
                (middle, False, False, True),
                (end, False, True, False),
            ]

        for frame_num, is_start, is_end, is_middle in candidates:
            if frame_num not in seen_frames:
                seen_frames.add(frame_num)
                keyframes.append({
                    "frame_number": frame_num,
                    "timestamp_sec": frame_num / fps if fps > 0 else 0,
                    "scene_number": scene.scene_number,
                    "is_scene_start": is_start,
                    "is_scene_end": is_end,
                    "is_scene_middle": is_middle,
                })

    # Sort by frame number
    keyframes.sort(key=lambda x: x["frame_number"])
    console.print(f"  → Selected [bold green]{len(keyframes)}[/] keyframes ({strategy})")
    return keyframes


def is_black_frame(image: Image.Image, threshold: int = 15, ratio: float = 0.95) -> bool:
    """Check if a frame is mostly black (credits, transitions)."""
    gray = np.array(image.convert("L"))
    dark_pixels = (gray < threshold).sum()
    total_pixels = gray.shape[0] * gray.shape[1]
    return (dark_pixels / total_pixels) > ratio


def create_thumbnail(image: Image.Image, max_width: int = None) -> Image.Image:
    """Create a thumbnail version of a frame."""
    max_width = max_width or config.extraction.thumbnail_max_width
    w, h = image.size
    if w <= max_width:
        return image.copy()
    new_w = max_width
    new_h = int(h * (max_width / w))
    return image.resize((new_w, new_h), Image.LANCZOS)


def _decode_target_frames(video_path: Path, target_frames: set[int]) -> dict[int, Image.Image]:
    """
    Decode specific frames from a video using PyAV.

    Groups nearby targets into batches, seeks once per batch, and decodes
    forward to collect all targets in the batch. This avoids redundant
    decoding of frames between sparse targets.

    Returns a dict mapping frame_number → PIL Image (RGB).
    """
    container = av.open(str(video_path))
    stream = container.streams.video[0]
    # Enable multi-threaded decoding (i9-10920X has 24 threads)
    stream.codec_context.thread_type = "AUTO"

    fps = float(stream.average_rate) if stream.average_rate else 25.0
    time_base = stream.time_base
    decoded: dict[int, Image.Image] = {}
    remaining = set(target_frames)

    # Group nearby targets into batches (within ~2 seconds gap → single seek)
    sorted_targets = sorted(remaining)
    gap_threshold = int(fps * 2)
    i = 0

    while i < len(sorted_targets):
        # Build batch: consecutive targets within gap_threshold of each other
        batch_end = i
        while (batch_end + 1 < len(sorted_targets)
               and sorted_targets[batch_end + 1] - sorted_targets[batch_end] < gap_threshold):
            batch_end += 1
        max_frame_in_batch = sorted_targets[batch_end]

        # Seek to the first target in this batch
        target_pts = int(sorted_targets[i] / fps / time_base)
        container.seek(target_pts, stream=stream)

        # Decode forward, collecting targets until we pass the batch
        for frame in container.decode(stream):
            frame_idx = frame.pts * time_base * fps if frame.pts is not None else -1
            frame_num = round(float(frame_idx))

            if frame_num in remaining:
                decoded[frame_num] = frame.to_image()  # PIL RGB
                remaining.discard(frame_num)

            if not remaining or frame_num > max_frame_in_batch + 2:
                break

        i = batch_end + 1

    container.close()
    return decoded


def extract_frames(
    video_path: Path,
    output_dir: Path,
    keyframes: list[dict],
    video_info: dict,
) -> list[FrameInfo]:
    """
    Extract keyframe images from video file and save to disk.

    Uses PyAV for video decoding and Pillow for image saving.
    Returns list of FrameInfo with file paths and metadata.
    """
    img_format = config.extraction.image_format
    img_quality = config.extraction.image_quality
    frames_dir = output_dir / "frames"
    thumbs_dir = output_dir / "thumbnails"
    frames_dir.mkdir(parents=True, exist_ok=True)
    thumbs_dir.mkdir(parents=True, exist_ok=True)

    # Decode all target frames from video in one pass
    target_frames = {kf["frame_number"] for kf in keyframes}
    console.print(f"  → Decoding {len(target_frames)} frames from video...")
    decoded = _decode_target_frames(video_path, target_frames)

    extracted = []
    skipped_black = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Saving frames...", total=len(keyframes))

        for kf in keyframes:
            frame_num = kf["frame_number"]
            image = decoded.get(frame_num)

            if image is None:
                progress.advance(task)
                continue

            # Skip black frames
            if is_black_frame(image):
                skipped_black += 1
                progress.advance(task)
                continue

            # File paths
            scene_str = f"scene{kf['scene_number']:04d}"
            frame_str = f"frame{frame_num:08d}"
            img_filename = f"{scene_str}_{frame_str}.{img_format}"

            img_path = frames_dir / img_filename
            thumb_path = thumbs_dir / img_filename

            # Save full frame
            save_kwargs = {"quality": img_quality} if img_format == "jpg" else {}
            image.save(str(img_path), **save_kwargs)

            # Save thumbnail
            thumb = create_thumbnail(image)
            thumb_kwargs = {"quality": config.extraction.thumbnail_quality} if img_format == "jpg" else {}
            thumb.save(str(thumb_path), **thumb_kwargs)

            timestamp_sec = kf["timestamp_sec"]
            w, h = image.size

            extracted.append(FrameInfo(
                frame_number=frame_num,
                timestamp_sec=timestamp_sec,
                timestamp_str=format_timestamp(timestamp_sec),
                scene_number=kf["scene_number"],
                is_scene_start=kf["is_scene_start"],
                is_scene_end=kf["is_scene_end"],
                is_scene_middle=kf["is_scene_middle"],
                image_path=str(img_path.relative_to(output_dir.parent)),
                thumbnail_path=str(thumb_path.relative_to(output_dir.parent)),
                width=w,
                height=h,
                file_size_kb=img_path.stat().st_size / 1024,
            ))

            progress.advance(task)

    if skipped_black > 0:
        console.print(f"  → Skipped [yellow]{skipped_black}[/] black frames")
    console.print(f"  → Extracted [bold green]{len(extracted)}[/] frames to {frames_dir}")

    return extracted


def run_extraction(
    video_path: str | Path,
    season: Optional[int] = None,
    episode_number: Optional[int] = None,
    title: Optional[str] = None,
    series_name: str = "The Simpsons",
    output_base: Optional[Path] = None,
) -> dict:
    """
    Full extraction pipeline: video → scenes → keyframes → images + metadata DB.

    Returns summary dict with episode_id, frame_count, scene_count, etc.
    """
    video_path = Path(video_path)
    if not video_path.exists():
        raise FileNotFoundError(f"Video not found: {video_path}")

    config.ensure_dirs()
    db = Database()

    start_time = time.time()
    console.print(f"\n[bold]{'='*60}[/]")
    console.print(f"[bold]Animation Asset Intelligence — Frame Extraction[/]")
    console.print(f"[bold]{'='*60}[/]")
    console.print(f"  Input: {video_path.name}")

    # ── Step 1: Video info ──
    console.print(f"\n[bold cyan]Step 1:[/] Reading video metadata...")
    video_info = get_video_info(video_path)
    console.print(f"  → {video_info['width']}x{video_info['height']} @ {video_info['fps']:.1f}fps")
    console.print(f"  → Duration: {format_timestamp(video_info['duration_sec'])} ({video_info['duration_sec']:.1f}s)")
    console.print(f"  → Size: {video_info['file_size_mb']:.1f} MB")

    # ── Step 2: Register episode in DB ──
    existing = db.get_episode_by_filename(video_path.name)
    if existing:
        console.print(f"\n[yellow]Episode already indexed (id={existing['id']}). Re-indexing...[/]")
        with db.connect() as conn:
            conn.execute("DELETE FROM episodes WHERE id = ?", (existing["id"],))

    episode_id = db.insert_episode(
        filename=video_path.name,
        series_name=series_name,
        season=season,
        episode_number=episode_number,
        title=title,
        duration_sec=video_info["duration_sec"],
        fps=video_info["fps"],
        width=video_info["width"],
        height=video_info["height"],
        file_path=str(video_path.absolute()),
        file_size_mb=video_info["file_size_mb"],
        status="processing",
    )
    console.print(f"  → Episode registered: id={episode_id}")

    # ── Step 3: Scene detection ──
    console.print(f"\n[bold cyan]Step 2:[/] Detecting scenes...")
    scenes = detect_scenes(video_path)

    # Fallback: if no scenes detected, treat entire video as one scene
    if not scenes:
        total_frames = video_info["total_frames"]
        duration = video_info["duration_sec"]
        console.print("  → [yellow]No scene cuts detected. Treating entire video as single scene.[/]")
        scenes = [SceneInfo(
            scene_number=1,
            start_frame=0,
            end_frame=total_frames - 1,
            start_time_sec=0.0,
            end_time_sec=duration,
            duration_sec=duration,
        )]

    # Save scenes to DB
    scene_records = []
    for s in scenes:
        scene_records.append({
            "episode_id": episode_id,
            "scene_number": s.scene_number,
            "start_time_sec": s.start_time_sec,
            "end_time_sec": s.end_time_sec,
            "start_frame": s.start_frame,
            "end_frame": s.end_frame,
        })
    db.insert_scenes_batch(scene_records)

    # ── Step 4: Select keyframes ──
    console.print(f"\n[bold cyan]Step 3:[/] Selecting keyframes...")
    keyframes = select_keyframes(scenes, video_info["fps"])

    # ── Step 5: Extract frame images ──
    # Output directory: data/<episode_dir>/
    episode_dir_name = video_path.stem  # e.g. "S01E01_Simpsons_Roasting"
    output_dir = output_base or (config.paths.data_dir / episode_dir_name)
    output_dir.mkdir(parents=True, exist_ok=True)

    console.print(f"\n[bold cyan]Step 4:[/] Extracting frame images...")
    frame_infos = extract_frames(video_path, output_dir, keyframes, video_info)

    # ── Step 6: Save frames to DB ──
    console.print(f"\n[bold cyan]Step 5:[/] Saving metadata to database...")

    # Get scene IDs from DB
    db_scenes = db.get_scenes(episode_id)
    scene_num_to_id = {s["scene_number"]: s["id"] for s in db_scenes}

    frame_records = []
    for fi in frame_infos:
        frame_records.append({
            "episode_id": episode_id,
            "scene_id": scene_num_to_id.get(fi.scene_number),
            "frame_number": fi.frame_number,
            "timestamp_sec": fi.timestamp_sec,
            "timestamp_str": fi.timestamp_str,
            "image_path": fi.image_path,
            "thumbnail_path": fi.thumbnail_path,
            "width": fi.width,
            "height": fi.height,
            "file_size_kb": fi.file_size_kb,
            "is_scene_start": fi.is_scene_start,
            "is_scene_end": fi.is_scene_end,
            "is_scene_middle": fi.is_scene_middle,
        })
    db.insert_frames_batch(frame_records)

    # Update episode status
    db.update_episode(
        episode_id,
        status="done",
        total_frames=len(frame_infos),
        total_scenes=len(scenes),
    )

    elapsed = time.time() - start_time

    # ── Summary ──
    summary = {
        "episode_id": episode_id,
        "filename": video_path.name,
        "scenes_detected": len(scenes),
        "keyframes_extracted": len(frame_infos),
        "output_dir": str(output_dir),
        "elapsed_sec": elapsed,
    }

    console.print(f"\n[bold]{'='*60}[/]")
    table = Table(title="Extraction Complete", show_header=False)
    table.add_column("Key", style="cyan")
    table.add_column("Value", style="green")
    table.add_row("Episode ID", str(episode_id))
    table.add_row("Scenes", str(len(scenes)))
    table.add_row("Keyframes", str(len(frame_infos)))
    table.add_row("Output", str(output_dir))
    table.add_row("Time", f"{elapsed:.1f}s")
    console.print(table)

    return summary


# ── CLI entry point ──
def main():
    parser = argparse.ArgumentParser(
        description="Extract keyframes from animation episode MP4",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.pipeline.extract_frames ./data/episodes/S01E01.mp4
  python -m src.pipeline.extract_frames ./data/episodes/S01E01.mp4 --season 1 --episode 1 --title "Simpsons Roasting"
  python -m src.pipeline.extract_frames ./data/episodes/S01E01.mp4 --threshold 30 --strategy first_last
        """,
    )
    parser.add_argument("video", type=str, help="Path to MP4 episode file")
    parser.add_argument("--season", "-s", type=int, default=None, help="Season number")
    parser.add_argument("--episode", "-e", type=int, default=None, help="Episode number")
    parser.add_argument("--title", "-t", type=str, default=None, help="Episode title")
    parser.add_argument("--series", type=str, default="The Simpsons", help="Series name")
    parser.add_argument("--threshold", type=float, default=None, help="Scene detection threshold (default: 27.0)")
    parser.add_argument("--strategy", type=str, default=None,
                        choices=["first_middle_last", "first_last", "first"],
                        help="Keyframe selection strategy")
    parser.add_argument("--output", "-o", type=str, default=None, help="Output directory")

    args = parser.parse_args()

    # Override config if CLI args provided
    if args.threshold:
        config.extraction.scene_threshold = args.threshold
    if args.strategy:
        config.extraction.keyframe_strategy = args.strategy

    output_base = Path(args.output) if args.output else None

    try:
        summary = run_extraction(
            video_path=args.video,
            season=args.season,
            episode_number=args.episode,
            title=args.title,
            series_name=args.series,
            output_base=output_base,
        )
    except Exception as e:
        console.print(f"\n[bold red]Error:[/] {e}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()