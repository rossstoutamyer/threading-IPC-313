#include <iostream>
#include <string.h>
#include <iomanip>
#include <mutex>
#include <condition_variable>
using namespace std;

#include "Histogram.h"

Histogram::Histogram(int _nbins, double _start, double _end): nbins (_nbins), start(_start), end(_end){
	//memset (hist, 0, nbins * sizeof (int));	
	hist = vector<int> (nbins, 0);
}
Histogram::~Histogram(){
}
void Histogram::update (double value){
	//cout << nbins << endl;
	int bin_index = (int) ((value - start) / (end - start) * nbins);
	//cout << "index: " << bin_index << endl;
	if (bin_index < 0)
		bin_index= 0;
	else if (bin_index >= nbins)
		bin_index = nbins-1;

	//cout << value << "-" << bin_index << endl;
	//locker.lock();
	hist [bin_index] ++;
	//locker.unlock();
}
vector<int> Histogram::get_hist(){
	return hist;
}

vector<double> Histogram::get_range (){
	vector<double> r;
	r.push_back (start);
	r.push_back (end);
	return r;
}
int Histogram::size(){
	return nbins;		
}