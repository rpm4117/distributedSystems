#include <iostream>
#include <vector>
#include <string>
#include <sstream>

using namespace std;

int main(int argc, char **argv) {
    string tuple = string(argv[1]);
    int index = tuple.find(" ");
    // cout<<std::stof("0.0112")<<"\n";
    long k = std::stol(tuple.substr(0, index));
    int v = std::stoi(tuple.substr(index + 1));

    if(v < 140)
        cout << k << " " << v << endl;

    return 0;
}
