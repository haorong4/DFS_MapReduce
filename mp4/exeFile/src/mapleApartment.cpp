#include <iostream>
#include <string>
#include <bits/stdc++.h>
#include <map>
#include <vector>
#include <string.h>
#include <stdio.h>

using namespace std;

int main() {
    map<string, int> mymap;
    while(1) {
        if (cin.peek() == EOF) {
            break;
        }
        string line; 
        getline(std::cin, line);
        if (line == "") {
            continue;
        }
        char* cstr = const_cast<char*>(line.c_str());
        size_t len = strlen(cstr);
        cstr[len] = '\0';
        string key = "Champaign";
        int value = 0;
        for(int i = 0; i < len; i++) {
            if (cstr[i] == '/'){
                cstr += i + 1;
                value = atoi(cstr);

                if (value > 10) {
                    value = 1;
                } else {
                    value = 0;
                }
                break;
            }

        }

        char* current;
        if ( mymap.find(key) == mymap.end() ) {
            // not found
            mymap[key] = value;
        } else {
            // found  
            mymap[key] += value;  
        } 
        
    }
    for (map<string,int>::iterator it=mymap.begin(); it!=mymap.end(); ++it)
        cout << it->first << " " << it->second << '\n'; 
}