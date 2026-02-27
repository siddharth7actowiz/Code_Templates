import os
base_path="Bade_Dir"
ext="txt"
numbers_of_folders=5
number_of_files_per_folder=10

for folder in range(1,numbers_of_folders+1):
    folder_name=f"folder_{folder}"
    folder_path=os.path.join(base_path,folder_name)
    os.makedirs(folder_path,exist_ok=True) #folders from 0-5

    for file in range(1,number_of_files_per_folder+1):
        file_name=f"filename_{file}.{ext}"
        file_path=os.path.join(folder_path,file_name)
        with open(file_path,"w")as f:
            pass
