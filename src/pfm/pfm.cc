#include "src/include/pfm.h"

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
        std::fstream file;

        // Try open at input mode. If successful, file already exists
        file.open(fileName, std::ios::in | std::ios::binary);
        if (file.is_open()) {
            return -1;
        }
        // If unsuccessful, create empty file at output mode
        else {
            file.open(fileName, std::ios::out | std::ios::binary);
            file.close();
            return 0;
        }
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        std::fstream file;

        // Try open at input mode. If unsuccessful, file doesn't exist
        file.open(fileName, std::ios::in | std::ios::binary);
        if (!file.is_open()) {
            return -1;
        }
        // If unsuccessful, delete the file
        else {
            remove(fileName.c_str());
            return 0;
        }
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        return fileHandle.setFile(fileName);
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        return fileHandle.closeFile();
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;

        pageNum = 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::setFile(const std::string &fileName) {
        // Try open at input mode. If successful, file already exists
        fileToBeHandled.open(fileName, std::ios::in | std::ios::out | std::ios::binary);
        if (fileToBeHandled.is_open()) {
            return 0;
        }
        else {
            return -1;
        }
    }

    RC FileHandle::closeFile() {
        if (fileToBeHandled.is_open()) {
            fileToBeHandled.close();
            return 0;
        }
        else {
            return -1;
        }
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        return -1;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        return -1;
    }

    RC FileHandle::appendPage(const void *data) {
        return -1;
    }

    unsigned FileHandle::getNumberOfPages() {
        return -1;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        return -1;
    }

} // namespace PeterDB