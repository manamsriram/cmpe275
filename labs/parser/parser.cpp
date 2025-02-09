/**
 * how to compile using gcc or clang
 *
 * COMPILING
 * compiling the code with debug symbols. The binary (executable) is 
 * parser. Replace -g with -O2 for optimized (choices 0,1,2,3)
 *
 * > <gcc-install-dir>/bin/g++ -std=c++11 -Wall -Wextra parser.cpp -g -o parser
 * > ./parser
 *
 * > <clang-install-dir>/bin/clang++ -std=c++11 -Wall -Wextra parser.cpp -g -o parser
 * > ./parser
 *
 * LINTER
 * check for common coding mistakes
 *
 * > clang++ -MJ -std=c++11 -o parser.o parser.cpp
 * > sed -e '1s/^/[\n/' -e '$s/,$/\n]/' *.o.json > compile_commands.json
 * > clang-tidy -checks=*,modernize-* parser.cpp  
 *
 * If repeating after changing code, you may have to delete the compilation 
 * database and rerun all three commands.
 *
 * MEMORY CHECK
 * sanitize is ran to check for memory errors. Note gcc/g++ does not 
 * support sanitize on apple/arm
 *
 * > clang++ -Wall -Wextra -fsanitize=address -fsanitize-address-use-after-scope parser.cpp -g -o parser
 * > ./parser
 *
 * CODE ANALYSIS
 * to use clang's static analyzer (does not create a binary), -o is the 
 * directory where the analyzer's report (*.html) files are found.
 *
 * > <clang-install-dir>/bin/clang++ --analyze -Xclang -analyzer-output=html -g parser.cpp -o out
 * > cd out
 * > open report*.html
 *
 * or to view in scan-view:
 * > <clang-install-dir>/bin/scan-build <clang-install-dir/bin/clang++ -g parser.cpp -o out
 * output will show the command to run 'scan-view <path-to-report>'
 *
 * you will need to either have python defined or run like:
 * > python3 <clang-install-dir>/bin/scan-view <path-to-report>
 */

#include <iostream>
#include <string>
#include <vector>
#include <regex>

/**
 * a simple test for clang's static analyzer from presention by Vince Bridgers.
 * ref: https://llvm.org/devmtg/2020-09/slides/Using_the_clang_static_ananalyzer_to_find_bugs.pdf
 */
int check(int b) {
    int r,z = 0;

    switch (b) {
        case 1: r = b/z; break;
        case 2: r = b+2; 
        case 3: r = b+3; break;
    }

    return r;
}

/**
 * a more complex test for clang's sanitize
 */
void splitter(const char *buf, int n) {
    std::vector<std::string> results; 
    const char* current_string = &buf[0];
    int c = 0;
    while (*current_string)
    {
        c++;
        if (c>n) break;
        results.push_back(current_string);
        current_string += results.back().length() + 1;
    }

    for (auto w : results) {
      std::cout << w << std::endl;
    }
}


int main() {
    // Note to test sanitize, remove the ending \0, compile, and run. The santitizer should 
    // create an error about global shadow memory.

    // const char *raw = "cats\0dogs\0fish\0chickens\0goats\0";
    const char *raw = "cats\0dogs\0fish\0chickens\0goats";

    splitter(raw,20);

    return 0;
}
