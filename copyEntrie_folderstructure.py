#copy entrie structure
import shutil
import os

source = 
destination =path

shutil.copytree(source, destination, dirs_exist_ok=True)

print("All folders and files copied successfully")


#copy only fies

os.makedirs(destination, exist_ok=True)

for root, dirs, files in os.walk(source):
    for file in files:
        source_file = os.path.join(root, file)
        dest_file = os.path.join(destination, file)

        shutil.copy2(source_file, dest_file)

print("All files copied to DEST_DIR")

#sing rename
import os

source = r"D:\Siddharth\osPractice\Bade_Dir"
destination = r"D:\Siddharth\osPractice\DEST_DIR"

for root, dirs, files in os.walk(source):
    
    # Create corresponding folder in destination
    relative_path = os.path.relpath(root, source)
    dest_folder = os.path.join(destination, relative_path)
    os.makedirs(dest_folder, exist_ok=True)

    for file in files:
        source_file = os.path.join(root, file)
        dest_file = os.path.join(dest_folder, file)

        os.rename(source_file, dest_file)

print("All files moved with same structure!")

