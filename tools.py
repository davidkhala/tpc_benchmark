"""Specific tools for EDA or debugging"""


def pathlist(directory_path, extension="*"):
    """Get all files in a directory, non-recursively"""
    from pathlib import Path
    return [str(fp) for fp in Path(directory_path).rglob(extension)]

def pathlist_recursive(directory_path, extension="*"):
    """Get all files in a directory, recursively"""
    import glob
    files = glob.glob(directory_path + '/**/*.' + extension, recursive=True)
    return files