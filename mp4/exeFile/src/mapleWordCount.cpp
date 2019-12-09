#include <iostream>
#include <string>
#include <bits/stdc++.h>
#include <map>
#include <vector>

using namespace std;

bool helper(char a) {
    if ((a <= 90 && a >= 65) || (a >= 97 && a <= 122)) {
        return false;
    } else {
        return true;
    }
}

int main() {
    map<string, int> mymap;
    while(1) {
        if (cin.peek() == EOF) {
            break;
        }
        string line; 
        getline(std::cin, line);
        if (line=="@") {
            break;
        }
        stringstream iss(line);
        vector<string> results(istream_iterator<string>{iss}, istream_iterator<string>());
        for (int i = 0; i < results.size(); i++) {
            string s = results[i];
            s.erase(remove_if(s.begin(), s.end(), helper), s.end());
            if (mymap.count(s) == 0) {
                mymap[s] = 1;
            } else {
                mymap[s] += 1;
            }
        }
    }
    for (map<string,int>::iterator it=mymap.begin(); it!=mymap.end(); ++it)
        cout << it->first << " " << it->second << "\n"; 
}