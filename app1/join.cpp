#include <iostream>
#include <vector>
#include <string>
#include <sstream>

using namespace std;

int main(int argc, char **argv) {
    string tuple = string(argv[1]);
    string dbtuple = string(argv[2]);
    int index = tuple.find(" ");
    // cout<<std::stof("0.0112")<<"\n";
    float k = std::stof(tuple.substr(0, index));
    // int v = std::stoi(tuple.substr(index + 1));

    int db_index1 = dbtuple.find(" ");
    int db_index2 = dbtuple.find(" ", db_index1 + 1);
    int v = std::stoi(dbtuple.substr(db_index1 + 1).substr(0, db_index2 - db_index1));

    cout << k << " " << v << " " << dbtuple << endl;

    return 0;
}
