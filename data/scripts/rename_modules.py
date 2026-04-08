import os
import glob
import re

ROOT = 'd:/ALPHA'
PY_FILES = [f for f in glob.glob(os.path.join(ROOT, 'modules', '*.py'))]

PATTERN = re.compile(r'def _process\(self, topic, message\):')
REPLACEMENT = 'def process(self, topic, message):'

def rename():
    print(f"Found {len(PY_FILES)} module files.")
    count = 0
    for f in PY_FILES:
        with open(f, 'r', encoding='utf-8') as file:
            content = file.read()
        
        if PATTERN.search(content):
            new_content = PATTERN.sub(REPLACEMENT, content)
            with open(f, 'w', encoding='utf-8') as file:
                file.write(new_content)
            count += 1
            print(f"Renamed _process in {os.path.basename(f)}")
            
    print(f"Successfully aligned {count} modules.")

if __name__ == "__main__":
    rename()
