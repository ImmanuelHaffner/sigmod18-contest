#include "Relation.hpp"

#include <err.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>


Relation::Relation(std::string filename)
    : filename_(filename)
{
    /* Load the relation by mmap()-ing the relation file. */
    fd_ = open(filename_.c_str(), O_RDONLY);
    if (fd_ == -1)
        err(EXIT_FAILURE, "Failed to open relation file \"%s\"", filename_.c_str());
    std::cerr << "opened " << filename << " with fd=" << fd_ << '\n';

    struct stat buf;
    if (fstat(fd_, &buf))
        err(EXIT_FAILURE, "Failed to get file status of relation file \"%s\"", filename_.c_str());
    size_ = buf.st_size;

    data_ = static_cast<uint64_t*>(mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd_, 0));
    if (MAP_FAILED == data_)
        err(EXIT_FAILURE, "Failed to load relation file \"%s\"", filename_.c_str());
}

Relation::~Relation()
{
    if (munmap((void*) data_, size_))
        warn("Failed unmapping the relation file \"%s\"", filename_.c_str());
    if (close(fd_))
        warn("Failed closing the relation file \"%s\"", filename_.c_str());
}
