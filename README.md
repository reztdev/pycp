# 📂 pycp – Python Copy Command

`pycp` is a Python-based implementation of the Linux/Unix `cp` command.  
It is built only with Python’s standard library and is designed to mimic basic functionality of `cp`, including file and directory copying, recursive operations, and overwrite control.

---

## 📝 Description
The purpose of `pycp` is to demonstrate how the core logic of file operations can be implemented in pure Python.  
While the native `cp` command in Linux is highly optimized and written in C, `pycp` offers a transparent, educational, and portable alternative that runs anywhere Python is available.

---

## 🚀 Features
- Copy a **single file** to another file or into a directory.
- Copy **multiple files** into a target directory.
- Copy **directories recursively** with `-r` / `--recursive`.
- Force overwrite existing files with `-f` / `--force`.
- Display progress of copied files with `-v` / `--verbose`.

---

## ⚡ Performance
- Since `pycp` is written in Python, it is generally **slower** than the native `cp` command written in C.
- For **small to medium files**, the performance difference is minimal.
- For **large files or directories with thousands of entries**, expect reduced speed compared to native tools.
- Best suited for **learning purposes, scripting, or portability** across platforms where `cp` may not be available.

---

## 🛠 Usage

### Basic syntax
```bash
python3 pycp.py [OPTIONS] SOURCE... DESTINATION
```

## ⚠️ Notes

- pycp is not a full replacement for the native cp command.

- It is mainly intended as an educational project and lightweight utility.

- Use with caution when applying -f (force overwrite), as files will be replaced without confirmation.
