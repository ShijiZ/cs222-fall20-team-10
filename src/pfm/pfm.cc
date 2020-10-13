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
        //fileToBeHandled = NULL;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::setFile(const std::string &fileName) {
        // Try open at input mode. If successful, file already exists
        fileToBeHandled->open(fileName, std::ios::in | std::ios::out | std::ios::binary);
        if (fileToBeHandled->is_open()) {
            return 0;
        }
        else {
            return -1;
        }
    }

    RC FileHandle::closeFile() {
        if (fileToBeHandled->is_open()) {
            fileToBeHandled->close();
            return 0;
        }
        else {
            return -1;
        }
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if (pageNum < this->pageNum) {
            fileToBeHandled->seekg((1 + pageNum) * PAGE_SIZE);
            fileToBeHandled->read(static_cast<char*>(data) , PAGE_SIZE);
            readPageCounter++;
            writeHiddenPage();
            return 0;
        }
        else {
            return -1;
        }
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        if (pageNum < this->pageNum) {
            char* buffer = new char[pageNum];
            std::memcpy(buffer, data, PAGE_SIZE);
            fileToBeHandled->seekp((1 + pageNum) * PAGE_SIZE);
            fileToBeHandled->write(buffer,PAGE_SIZE);
            writePageCounter++;
            writeHiddenPage();
            return 0;
        }
        else{
            return -1;
        }
    }

    RC FileHandle::appendPage(const void *data) {
        char* buffer = new char[pageNum];
        std::memcpy(buffer, data, PAGE_SIZE);
        fileToBeHandled->seekp(0,std::ios::end);
        fileToBeHandled->write(buffer,PAGE_SIZE);
        appendPageCounter++;
        pageNum++;
        writeHiddenPage();
        delete[] buffer;
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        return pageNum;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readHiddenPage();
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        return 0;
    }

    void FileHandle::readHiddenPage() {
        char* buffer = new char[16];
        fileToBeHandled->seekg(0,std::ios::beg);
        fileToBeHandled->read(buffer,16);

        readPageCounter = static_cast<unsigned>(static_cast<unsigned char>(buffer[0]));
        writePageCounter = static_cast<unsigned>(static_cast<unsigned char>(buffer[1]));
        appendPageCounter = static_cast<unsigned>(static_cast<unsigned char>(buffer[2]));
        pageNum = static_cast<unsigned>(static_cast<unsigned char>(buffer[3]));
        delete[] buffer;
    }

    void FileHandle::writeHiddenPage() {
        char* buffer = new char[16];
        buffer[0] = static_cast<char>(readPageCounter);
        buffer[1] = static_cast<char>(writePageCounter);
        buffer[2] = static_cast<char>(appendPageCounter);
        buffer[3] = static_cast<char>(pageNum);
        fileToBeHandled->seekp(0, std::ios::beg);
        fileToBeHandled->write(buffer, PAGE_SIZE);
        delete[] buffer;
    }

} // namespace PeterDB