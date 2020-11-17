#include "src/include/pfm.h"
#include <iostream>

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
            initHiddenPage(file);
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
        // If successful, delete the file
        else {
            file.close();
            remove(fileName.c_str());
            return 0;
        }
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        return fileHandle.openFile(fileName);
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        return fileHandle.closeFile();
    }

    void PagedFileManager::initHiddenPage(std::fstream& file) {
        unsigned readPageCounter = 0;
        unsigned writePageCounter = 0;
        unsigned appendPageCounter = 1;
        unsigned numPages = 0;

        unsigned* buffer = new unsigned[PAGE_SIZE/sizeof(unsigned)];
        buffer[0] = readPageCounter;
        buffer[1] = writePageCounter;
        buffer[2] = appendPageCounter;
        buffer[3] = numPages;
        file.seekp(0, std::ios::beg);
        file.write((char*) buffer, PAGE_SIZE);
        delete[] buffer;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;

        numPages = 0;
        fileToBeHandled = new std::fstream;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::openFile(const std::string &fileName) {
        // Test if fileToBeHandled is already open
        if (fileToBeHandled->is_open()) {
            return -1;
        }
        fileToBeHandled->open(fileName, std::ios::in | std::ios::out | std::ios::binary);
        if (fileToBeHandled->is_open()) {
            readHiddenPage();
            return 0;
        }
        else {
            return -2; //file not exists
        }
    }

    RC FileHandle::closeFile() {
        // If file is already closed, do nothing

        //if (fileToBeHandled->is_open()) {
        writeHiddenPage();
        fileToBeHandled->close();
        return 0;
        //}
        //else {
        //    return -1;
        //}
    }

    RC FileHandle::readPage(PageNum pageNum, void* data) {
        if (pageNum < numPages) {
            fileToBeHandled->seekg((1+pageNum)*PAGE_SIZE);
            fileToBeHandled->read((char*) data , PAGE_SIZE);
            readPageCounter++;
            //writeHiddenPage();
            return 0;
        }
        else {
            return -1;
        }
    }

    RC FileHandle::writePage(PageNum pageNum, const void* data) {
        if (pageNum < numPages) {
            fileToBeHandled->seekp((1+pageNum)*PAGE_SIZE);
            fileToBeHandled->write((char*) data, PAGE_SIZE);
            writePageCounter++;
            //writeHiddenPage();
            return 0;
        }
        else{
            return -1;
        }
    }

    RC FileHandle::appendPage(const void* data) {
        fileToBeHandled->seekp(0, std::ios::end);
        fileToBeHandled->write((char*) data, PAGE_SIZE);
        appendPageCounter++;
        numPages++;
        writeHiddenPage();
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        return numPages;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        //readHiddenPage();
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        return 0;
    }

    void FileHandle::readHiddenPage() {
        char* buffer = new char[PAGE_SIZE];
        fileToBeHandled->seekg(0,std::ios::beg);
        fileToBeHandled->read(buffer,4*sizeof(unsigned));

        readPageCounter = ((unsigned*) buffer)[0];
        writePageCounter = ((unsigned*) buffer)[1];
        appendPageCounter = ((unsigned*) buffer)[2];
        numPages = ((unsigned*) buffer)[3];
        delete[] buffer;

        readPageCounter++;  // increment because hidden page is read
    }

    void FileHandle::writeHiddenPage() {
        writePageCounter++; // increment because hidden page is written

        unsigned* buffer = new unsigned[PAGE_SIZE/sizeof(unsigned)];
        // buffer[0] = static_cast<char>(readPageCounter);
        buffer[0] = readPageCounter;
        buffer[1] = writePageCounter;
        buffer[2] = appendPageCounter;
        buffer[3] = numPages;
        fileToBeHandled->seekp(0, std::ios::beg);
        fileToBeHandled->write((char*) buffer, PAGE_SIZE);
        delete[] buffer;
    }

} // namespace PeterDB