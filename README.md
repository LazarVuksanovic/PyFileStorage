# PyFileStorage

PyFileStorage is a Python project that leverages both multiprocessing and threading to efficiently manage file system operations. This application provides a command-line interface for users to interact with the file storage system, allowing the storage, retrieval, and deletion of files. The utilization of multiprocessing enhances the performance of file part processing, while threading ensures a responsive user interface.

## Features

- **Put Files:** Store files in the file register, breaking them into parts for optimal storage and retrieval.
- **Get Files:** Retrieve files from the file register, reconstructing them from their parts.
- **Delete Files:** Remove files from the file register, including the deletion of their individual parts.
- **List Files:** Display a list of all files in the file register.

## Technologies Used

- **Python:** Core language used for development.
- **Multiprocessing:** Utilized for parallel processing of file parts, improving overall performance.
- **Threading:** Employed for concurrent user input handling, ensuring a responsive interface.
- **YAML:** Configuration file for specifying system parameters.

## Configuration

Configure system parameters by editing the `config.yaml` file:

- `uiprocesses`: Maximum number of user interface processes.
- `directory`: Base directory for storing file parts.
- `maxmemory`: Maximum memory limit for file processing.
