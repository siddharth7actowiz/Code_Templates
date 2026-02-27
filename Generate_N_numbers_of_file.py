import os

# ====== CONFIGURATION ======
base_path = "D:/test_folders"   # change to your path
num_folders = 5                 # N folders
files_per_folder = 3            # M files inside each folder
# ============================

# Create base directory if not exists
os.makedirs(base_path, exist_ok=True)

for i in range(1, num_folders + 1):
    folder_path = os.path.join(base_path, f"Folder_{i}")
    os.makedirs(folder_path, exist_ok=True)

    for j in range(1, files_per_folder + 1):
        file_path = os.path.join(folder_path, f"file_{j}.txt")

        with open(file_path, "w") as f:
            f.write(f"This is file {j} inside Folder {i}")

print("Folders and files created successfully!")
