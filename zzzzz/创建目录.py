import os


def create_project_structure(base_dir):
    """Create the ecommerce-price-index project structure"""

    # Define the directory structure
    structure = {
        'config': ['__init__.py', 'settings.py', 'constants.py'],
        'data_generation': ['__init__.py', 'generator.py', 'validator.py'],
        'storage': ['__init__.py', 'oss_connector.py', 'clickhouse_connector.py'],
        'processing': ['__init__.py', 'data_cleaning.py', 'schema_mapping.py', 'transformer.py'],
        'analysis': ['__init__.py', 'price_index.py', 'visualization.py'],
        'utils': ['__init__.py', 'logger.py', 'date_utils.py'],
        '.': ['创建目录.py', 'requirements.txt']  # Files in root directory
    }

    # Create directories and files
    for directory, files in structure.items():
        dir_path = os.path.join(base_dir, directory)

        # Create directory if it doesn't exist
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
            print(f"Created directory: {dir_path}")

        # Create files in the directory
        for file in files:
            file_path = os.path.join(dir_path, file)
            if not os.path.exists(file_path):
                with open(file_path, 'w') as f:
                    pass  # Just create empty file
                print(f"Created file: {file_path}")


if __name__ == "__main__":
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Create the project structure in the same directory as this script
    create_project_structure(script_dir)

    print("\nProject structure created successfully!")