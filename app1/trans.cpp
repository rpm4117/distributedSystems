#include <iostream>
#include <vector>
#include <string>
#include <sstream>

using namespace std;

int main(int argc, char **argv) {
    string tuple = string(argv[1]);
    int index = tuple.find(" ");
    // cout<<std::stof("0.0112")<<"\n";
    float k = std::stof(tuple.substr(0, index));
    int v = std::stoi(tuple.substr(index + 1));

    cout << (long)k*1000 <<" "<<v;

    return 0;
}