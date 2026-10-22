CXX ?= c++
CPPFLAGS += -Isrc
CXXFLAGS ?= -O3 -DNDEBUG -std=c++17 -Wall -Wextra -Wpedantic -pthread

.PHONY: benchmark clean

build/kv_bench: bench/kv_bench.cpp $(wildcard src/*.cpp) $(wildcard src/*.h)
	@mkdir -p build
	@$(CXX) $(CPPFLAGS) $(CXXFLAGS) bench/kv_bench.cpp $(wildcard src/*.cpp) $(LDFLAGS) -o $@

benchmark: build/kv_bench
	@./build/kv_bench

build/basic: examples/basic.cpp $(wildcard src/*.cpp) $(wildcard src/*.h)
	@mkdir -p build
	@$(CXX) $(CPPFLAGS) $(CXXFLAGS) examples/basic.cpp $(wildcard src/*.cpp) $(LDFLAGS) -o $@

build/quorum: tools/cli.cpp $(wildcard src/*.cpp) $(wildcard src/*.h)
	@mkdir -p build
	@$(CXX) $(CPPFLAGS) $(CXXFLAGS) tools/cli.cpp $(wildcard src/*.cpp) $(LDFLAGS) -o $@

clean:
	rm -rf build
