# -*- coding: utf-8 -*-
import os
import sys
import re
import html

# 设置控制台编码（针对 Windows 系统）
if sys.platform.startswith('win'):
    sys.stdout.reconfigure(encoding='utf-8')

def extract_number(filename):
    # 首先解码 HTML 实体
    filename = html.unescape(filename)
    # 匹配类似 [5.1] 或 5.1 这样的序号模式
    match = re.search(r'\[?(\d+)\.(\d+)\]?', filename)
    if match:
        # 将章节号和小节号分别提取出来
        chapter = int(match.group(1))
        section = int(match.group(2))
        # 返回一个元组用于排序
        return (chapter, section)
    return (float('inf'), float('inf'))  # 如果没有序号，将其排到最后

def get_new_filename(old_name):
    # 解码 HTML 实体
    old_name = html.unescape(old_name)
    # 提取序号和描述部分，修改正则表达式以更精确匹配
    match = re.search(r'\[?(\d+)\.(\d+)\]?-*(.+?)(?:\.mp4)+$', old_name)
    if match:
        chapter = match.group(1)
        section = match.group(2)
        # 获取文件描述部分
        description = match.group(3).strip()
        # 移��描述开头的序号格式（比如 "5-10" 或 "5-10-"）
        description = re.sub(rf'^{chapter}-{section}[-\s]*', '', description)
        # 返回新文件名，在序号后添加空格，用【】包裹描述内容
        return f"{chapter}-{section}【{description}】.mp4"
    return old_name

def rename_videos_in_folder(folder_path):
    video_extensions = ('.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm')
    video_files = []
    
    # 获取当前文件夹中的视频文件
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)
        if os.path.isfile(file_path) and file.lower().endswith(video_extensions):
            video_files.append(file)
    
    if video_files:
        print(f"\n处理文件夹: {folder_path}")
        # 对当前文件夹的视频文件进行排序
        video_files.sort(key=extract_number)
        
        # 打印重命名计划
        print("\n准备重命名以下文件：")
        for old_name in video_files:
            new_name = get_new_filename(old_name)
            if old_name != new_name:  # 只显示需要改名的文件
                print(f"{old_name} -> {new_name}")
        
        # 询问用户是否继续
        confirm = input("\n是否继续重命名？(y/n): ")
        if confirm.lower() == 'y':
            # 执行重命名
            for old_name in video_files:
                new_name = get_new_filename(old_name)
                if old_name != new_name:  # 只重命名需要改名的文件
                    old_path = os.path.join(folder_path, old_name)
                    new_path = os.path.join(folder_path, new_name)
                    os.rename(old_path, new_path)
                    print(f"已重命名: {old_name} -> {new_name}")
        else:
            print("已跳过此文件夹的重命名操作")

def process_all_folders(root_path):
    try:
        # 遍历所有文件夹
        for root, dirs, files in os.walk(root_path):
            rename_videos_in_folder(root)
            
        print("\n所有文件夹处理完成！")
            
    except Exception as e:
        print(f"发生错误：{str(e)}")

if __name__ == "__main__":
    # 获取用户输入的根文件夹路径
    root_path = input("请输入要处理的根文件夹路径：")
    
    # 确保路径存在
    if os.path.exists(root_path):
        process_all_folders(root_path)
    else:
        print("文件夹路径不存在，请检查输入是否正确。") 