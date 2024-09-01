import os
import shutil
import hashlib
from src.error_handler import FileError

class FileManager:
    def __init__(self, base_dir):
        self.base_dir = base_dir

    def save_dataset(self, dataset_id, category, data):
        try:
            category_dir = os.path.join(self.base_dir, category)
            os.makedirs(category_dir, exist_ok=True)
            
            temp_file = os.path.join(category_dir, f"{dataset_id}_temp.csv")
            final_file = os.path.join(category_dir, f"{dataset_id}_latest.csv")
            
            with open(temp_file, "wb") as f:
                f.write(data)
            
            if not os.path.exists(final_file) or not self._files_are_identical(temp_file, final_file):
                shutil.move(temp_file, final_file)
                return final_file
            else:
                os.remove(temp_file)
                return None
        except Exception as e:
            raise FileError(f"Failed to save dataset {dataset_id}: {str(e)}")

    def _files_are_identical(self, file1, file2):
        return self._get_file_hash(file1) == self._get_file_hash(file2)

    def _get_file_hash(self, file_path):
        hasher = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()