#include <iostream>
#include <vector>
#include <string>
#include <sstream>

using namespace std;

int main(int argc, char **argv) {
    string tuple = string(argv[1]);
    int c_index_1 = tuple.find(",");
    int c_index_2 = tuple.find(",", c_index_1 + 1);
    string id = tuple.substr(0, c_index_1);
    string date = tuple.substr(c_index_1 + 1).substr(0, c_index_2 - c_index_1);
    string content = tuple.substr(c_index_2 + 1);
    // cout<<std::stof("0.0112")<<"\n";
    if(date.find("2017") != -1)
        cout<<content;

    return 0;
}
