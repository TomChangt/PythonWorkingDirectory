import os
import re


def rename_video_files(root_folders):
    video_extensions = (".mp4", ".MP4")

    for root_folder in root_folders:
        for dirpath, dirnames, filenames in os.walk(root_folder):
            for filename in filenames:
                if filename.lower().endswith(video_extensions):
                    new_name = process_filename(filename)

                    if new_name != filename:
                        old_path = os.path.join(dirpath, filename)
                        new_path = os.path.join(dirpath, new_name)
                        os.rename(old_path, new_path)
                        print(f"Renamed: {old_path} -> {new_path}")


def process_filename(filename):
    # Remove the part enclosed in【】brackets
    new_name = re.sub(r"【.*?】", "", filename)
    # Remove any trailing whitespace that might be left after removal
    new_name = new_name.rstrip()
    return new_name


if __name__ == "__main__":
    # Specify the root folders to start the traversal
    root_folders = [
        r"/Users/changtong/Downloads/Mybatis架构及源码解析",
        r"/Users/changtong/Downloads/RocketMQ",
        r"/Users/changtong/Downloads/Spring Boot",
        r"/Users/changtong/Downloads/Spring Cloud",
        r"/Users/changtong/Downloads/Spring Data源码解析",
    ]

    rename_video_files(root_folders)
    print("File renaming process completed.")
