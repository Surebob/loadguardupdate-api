import os
import logging
import zipfile
from datetime import datetime

class ZipProcessor:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Define which files to extract for each ZIP type
        self.extraction_patterns = {
            'FTP_Crash': lambda date: f"{date}_Crash.txt",
            'FTP_Inspection': lambda date: f"{date}_Inspection.txt",
            'FTP_Violation': lambda date: f"{date}_Violation.txt",
            'SMS': lambda date: f"SMS_AB_PassProperty_{date}.txt"
        }

    def process_all_zips(self):
        """Process all ZIP files in their respective directories"""
        any_processed = False
        
        # Process each directory
        for dir_type in self.extraction_patterns.keys():
            dir_path = os.path.join(self.base_dir, dir_type)
            if not os.path.exists(dir_path):
                continue

            # Create Extracted subdirectory for all types
            extract_dir = os.path.join(dir_path, 'Extracted')
            os.makedirs(extract_dir, exist_ok=True)

            # Find ZIP files in directory
            zip_files = [f for f in os.listdir(dir_path) if f.endswith('.zip')]
            if not zip_files:
                continue

            for zip_file in zip_files:
                if self.process_zip(dir_type, zip_file):
                    any_processed = True

        return any_processed

    def process_zip(self, dir_type, zip_filename):
        """Process a single ZIP file"""
        try:
            # Extract date from filename
            if dir_type == 'SMS':
                date = zip_filename.split('_')[-1].split('.')[0]  # Extract '2024Sep' from 'SMS_AB_PassProperty_2024Sep.zip'
            else:
                date = zip_filename.split('_')[1].split('.')[0]  # Extract '2024Sep' from 'Crash_2024Sep.zip'

            # Get the target file pattern
            target_file = self.extraction_patterns[dir_type](date)
            zip_path = os.path.join(self.base_dir, dir_type, zip_filename)

            # All types now use Extracted subdirectory
            extract_dir = os.path.join(self.base_dir, dir_type, 'Extracted')

            self.logger.info(f"Processing {zip_filename}")
            
            # Open and extract specific file
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # List all files in ZIP
                file_list = zip_ref.namelist()
                
                # Find the target file (case insensitive)
                target_file_lower = target_file.lower()
                found_file = next(
                    (f for f in file_list if f.lower() == target_file_lower),
                    None
                )
                
                if found_file:
                    # Extract to the appropriate directory
                    zip_ref.extract(found_file, extract_dir)
                    
                    # Rename if case is different
                    extracted_path = os.path.join(extract_dir, found_file)
                    final_path = os.path.join(extract_dir, target_file)
                    if extracted_path != final_path:
                        os.rename(extracted_path, final_path)
                    
                    self.logger.info(f"Extracted {target_file} to {extract_dir}")
                    return True
                else:
                    self.logger.error(f"Target file {target_file} not found in {zip_filename}")
                    return False

        except Exception as e:
            self.logger.error(f"Error processing {zip_filename}: {str(e)}")
            return False
