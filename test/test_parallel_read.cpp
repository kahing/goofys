// test parallel read from multiple threads on the same file; mainly used for
// testing and benchmarking block read cache

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cstdint>

constexpr int NR_WORKERS = 4, NR_PARTS = 1024 * 1024 * 2;

class ParallelReader {
    std::string m_path;
    std::unique_ptr<uint8_t[]> m_buf;
    std::vector<std::pair<size_t, size_t>> m_parts;
    std::atomic_uint32_t m_next_part;
    size_t m_file_size;

    static size_t get_file_size(const char* fpath) {
        struct stat stat_buf;
        int rc = stat(fpath, &stat_buf);
        assert(rc == 0);
        return stat_buf.st_size;
    }

    // generate random m_parts
    void gen_parts(std::default_random_engine& rng, size_t tot_size) {
        // O(n) algorithm to permute objects and splits to generate a random
        // partition

        std::uniform_real_distribution<double> uniform{0.0, 1.0};
        size_t this_part = 1,
               nr_parts = std::min<size_t>(NR_PARTS, tot_size / 20),
               nr_split = nr_parts - 1, nr_obj = tot_size - nr_split - 1;

        while (nr_split) {
            if (uniform(rng) <=
                static_cast<double>(nr_obj) /
                        static_cast<double>(nr_obj + nr_split)) {
                ++this_part;
                --nr_obj;
            } else {
                m_parts.emplace_back(0, this_part);
                this_part = 1;
                --nr_split;
            }
        }
        m_parts.emplace_back(0, this_part + nr_obj);
        assert(m_parts.size() == nr_parts);

        size_t off = 0;
        for (size_t i = 0; i < nr_parts; ++i) {
            m_parts[i].first = off;
            assert(m_parts[i].second);
            off += m_parts[i].second;
        }
        assert(off == tot_size);

        std::shuffle(m_parts.begin(), m_parts.end(), rng);
    }

    void worker() {
        int fd = open(m_path.c_str(), O_RDONLY);
        assert(fd > 0);
        for (;;) {
            size_t job = m_next_part.fetch_add(1, std::memory_order_relaxed);
            if (job >= m_parts.size()) {
                break;
            }
            size_t off, size;
            std::tie(off, size) = m_parts[job];
            while (size > 0) {
                ssize_t r = pread(fd, m_buf.get() + off, size, off);
                assert(r > 0);
                size -= r;
                off += r;
            }
        }
        close(fd);
    }

public:
    ParallelReader(std::default_random_engine& rng, std::string path)
            : m_path{std::move(path)} {
        size_t fsize = get_file_size(m_path.c_str());
        m_file_size = fsize;
        gen_parts(rng, fsize);
        m_buf.reset(new uint8_t[fsize]);

        std::vector<std::thread> workers;
        for (int i = 0; i < NR_WORKERS; ++i) {
            workers.emplace_back([this]() { worker(); });
        }
        for (size_t done; (done = m_next_part.load(std::memory_order_relaxed)) <
                          m_parts.size();) {
            printf("\rreading: %zu/%zu   ", done, m_parts.size());
            fflush(stdout);
            usleep(500'000);
        }
        printf("\n");
        for (auto& i : workers) {
            i.join();
        }
    }

    void write(const char* path) {
        FILE* fout = fopen(path, "wb");
        assert(fout);
        size_t w = fwrite(m_buf.get(), 1, m_file_size, fout);
        assert(w == m_file_size);
        fclose(fout);
    }
};

int main(int argc, const char* const argv[]) {
    if (argc == 1 || argc % 2 != 1) {
        fprintf(stderr, "usage: %s <inp 0> <out 0> ...\n", argv[0]);
        return 1;
    }

    std::vector<std::thread> workers;
    std::random_device rd;
    for (int i = 1; i < argc; i += 2) {
        auto fn = [inp = argv[i], out = argv[i + 1], seed = rd()]() {
            std::default_random_engine rng{seed};
            ParallelReader r{rng, inp};
            r.write(out);
        };
        workers.emplace_back(fn);
    }

    for (auto& i : workers) {
        i.join();
    }
}

// vim: syntax=cpp.doxygen foldmethod=marker foldmarker=f{{{,f}}}
