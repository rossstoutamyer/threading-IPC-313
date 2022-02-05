#include "common.h"
#include "BoundedBuffer.h"
#include "Histogram.h"
#include "common.h"
#include "HistogramCollection.h"
#include "FIFOreqchannel.h"
#include <thread>
#include <fstream>

using namespace std;


void patient_thread_function(string filename, int patientNum, int numData, int mVal, BoundedBuffer* buf, FIFORequestChannel* channel, int w){
    /* What will the patient threads do? */
    if (filename == "") {
        //cout << "Thread here" << endl;
        datamsg x(patientNum, 0, 1); //create the datamsg
        for (int i = 0; i < numData; ++i) {
            buf->push((char*) &x, sizeof(datamsg)); //push datamsg onto buffer
            x.seconds += 0.004; //increment for next data point
            //cout << "Data : " << x.seconds << endl;
        }
    } else {
        // cout << "FILEMESSAGE" << endl;
        filemsg fm(0, 0);
        int len = sizeof (filemsg) + filename.size()+1;
        // cout << "Len: " << len << sizeof(filemsg) << endl;
        char buffer[len];
        memcpy(buffer, &fm, sizeof(filemsg));
        strcpy(buffer + sizeof (filemsg), filename.c_str());
        // cout << "write for file size" << endl;
        channel->cwrite (buffer, len);  // I want the file length;
	    __int64_t freply = 0;
        // cout << "Read file size" << endl;
	    channel->cread(&freply, sizeof(freply));
        // cout << "read " << freply << endl;
        
        string createdFile = "received/" + filename;
        FILE* file = fopen(createdFile.c_str(), "wb");
        fseek(file, freply, SEEK_SET);
        fclose(file);

        int i = 0;
        while (freply > 0) { //while the file is not done
            if (i > 0) { //find offset
                fm.offset += mVal;
            }else {
                fm.offset = 0;
            }
            if (freply >= mVal) { //if the file length is greater than max message, the length we want is max message
                fm.length = mVal;
            } else {
                fm.length = freply;
            }
            memcpy (buffer, &fm, sizeof (filemsg)); //copies sizeof(filemsg) characters from fm into buf2
            strcpy (buffer + sizeof (filemsg), filename.c_str()); //copies string from fname.c_str() to buf2 + sizeof(filemsg)
            buf->push((char*) buffer, len);
            freply -= mVal;
            ++i;
            //break;
        }
        // cout << "DONE!" << endl;
    }
}

void worker_thread_function(int mVal, string filename, BoundedBuffer* buf, BoundedBuffer* response, FIFORequestChannel* channel){
    /* Functionality of the worker threads */
    char buffer[mVal];
    //cout << "working" << endl;

    while(true) {
        // cout << "Popping" << endl;
        buf->pop(buffer, mVal);
        // cout << "Popped" << endl;
        MESSAGE_TYPE* message = (MESSAGE_TYPE*) buffer;

        if (*message == DATA_MSG) {
            datamsg* x = (datamsg*) buffer;
            double reply = 0;
            channel->cwrite(buffer, sizeof(datamsg));
            channel->cread(&reply, sizeof(double));
            string strReply = to_string(reply);
            strReply = to_string(x->person) + "=" + strReply;
            // cout << "Reply: " << reply << endl;
            response->push((char*)strReply.c_str(), sizeof(strReply));
        } else if (*message == FILE_MSG) {
            string createdFile = "received/" + filename;
            filemsg* newfm = (filemsg*) buffer;
            char buf3[mVal] = {};
            int len = sizeof (filemsg) + filename.size()+1;
            channel->cwrite(buffer, len);
            int fileBytes = channel->cread(&buf3, mVal);

            // ofstream fp(createdFile);
            // fp.seekp(newfm->offset);
            // fp << buf3;
            // fp.close();

            FILE* fp = fopen (createdFile.c_str(), "rb+");
            fseek(fp, newfm->offset, SEEK_SET);
            //cout << buf3 << endl << endl;
            fwrite(buf3, 1, fileBytes, fp);
            fclose(fp);
        } 
         else if(*message == QUIT_MSG) {
            // cout << "Deleting Channel" << endl;
            //cout << *finished << endl;
            channel->cwrite (message, sizeof (MESSAGE_TYPE));
            delete channel;
            break;
        }
    }
}
void histogram_thread_function (BoundedBuffer* response, int mVal, HistogramCollection* hc){
    /* Functionality of the histogram threads */
    //cout << "Using Responses" << endl; 
    char* buffer = new char[mVal];
    // cout << "Hthread" << endl;
    while (true) {
        //cout << response->size() << endl;
        //cout << "FINISHED " << *finished << endl;
        if (response->size() != 0) {
            response->pop(buffer, mVal);
            MESSAGE_TYPE* message = (MESSAGE_TYPE*) buffer;
            if (*message == QUIT_MSG) {
                break;
            }
            char* curr = buffer;
            //cout << "Buffer: " << buffer << endl;
            int pNo = 0;
            string stringpNo = "";
            // cout << "Buffer: " << buffer << endl;
            while(*buffer != '=') {
                buffer++;
            }
            (*buffer) = '\0';
            stringpNo = curr;
            pNo = stoi(stringpNo);
            buffer++;
            //cout << "PNo: " << pNo << endl;
            //cout << "New Buffer: " << buffer << endl;
            string strValue(buffer);
            double value = stod(strValue);
            //cout << "pNo: " << pNo << endl;
            hc->updateHist(pNo - 1, value);
            // cout << "Updated" << endl;
            buffer = curr;
        } //else {
            //cout << "EMPTY" << endl;
        //}
        //cout << "Updating histogram because finished is " << *finished << endl;
    }
    delete buffer;
}

int main(int argc, char *argv[])
{
    int opt;
    int n = 100;    		//default number of requests per "patient"
    int p = 10;     		// number of patients [1,15]
    int w = 100;    		//default number of worker threads
    int b = 20; 		// default capacity of the request buffer, you should change this default
    int h = 10;
	int mVal = MAX_MESSAGE; 	// default capacity of the message buffer
    string stringM = "256";
    string filename = "";
    srand(time_t(NULL));

    while((opt = getopt(argc, argv, "n:p:w:b:m:f:h:")) != -1) {
        switch (opt) {
            case 'n':
                n = atoi(optarg);
                break;
            case 'p':
                p = atoi(optarg);
                break;
            case 'w':
                w = atoi(optarg);
                break;
            case 'b':
                b = atoi(optarg);
                break;
            case 'm':
                mVal = atoi(optarg);
                stringM = to_string(mVal);
                break;
            case 'f':
                filename = optarg;
                break;
            case 'h':
                h = atoi(optarg);
                break;
        }
    }
    
    
    int pid = fork();
    if (pid == 0){
		// modify this to pass along m
        char *childArgs[] = {"./server", "-m", (char*)stringM.c_str(), NULL};
	    execvp(childArgs[0], childArgs);
        return 0;
    }
    
    int finished = 0;
	FIFORequestChannel* chan = new FIFORequestChannel("control", FIFORequestChannel::CLIENT_SIDE);
    BoundedBuffer* request_buffer = new BoundedBuffer(b);
    BoundedBuffer* response_buffer = new BoundedBuffer(b);
	HistogramCollection hc;
	
    if (filename != "") {
        p = 0;
        h = 0;
    }

	for (int i = 0; i < p; ++i) {
        Histogram* data = new Histogram(p, -2.0, 2.0);
        hc.add(data);
    }

    FIFORequestChannel** workers = new FIFORequestChannel*[w];
    for (int i = 0; i < w; ++i) {
        char* newBuffer = new char[mVal];
	    string cReply = "";
	    MESSAGE_TYPE newMsg = NEWCHANNEL_MSG;

        // cout << "Making new channel" << endl;
	    chan->cwrite(&newMsg, sizeof(MESSAGE_TYPE));
	    chan->cread(newBuffer, mVal);
	    cReply = string(newBuffer);
	  
	    FIFORequestChannel* newChan = new FIFORequestChannel(cReply, FIFORequestChannel::CLIENT_SIDE);
        // cout << "New channel made" << endl;
        workers[i] = newChan;
        delete newBuffer;
    }

    if (filename != "") {
        p = 1;
        h = 0;
    }
	
    struct timeval start, end;
    gettimeofday (&start, 0);
    /* Start all threads here */
    thread patients[p];
    if (filename == "") {
        cout << "Creating Data Threads..." << endl;
        for (int i = 0; i < p; ++i) {
            patients[i] = thread(patient_thread_function, filename, i + 1, n, mVal, request_buffer, chan, w);
            //patients[i].detach();
        }
    } else {
        // cout << "File Requesting thread " << filename << endl;
        cout << "Creating File Thread" << endl;
        patients[0] = thread(patient_thread_function, filename, 1, n, mVal, request_buffer, chan, w);
    }
    thread workerthread[w];
    cout << "Creating Worker Threads" << endl;
    for (int i = 0; i < w; ++i) {
        workerthread[i] = thread(worker_thread_function, mVal, filename, request_buffer, response_buffer, workers[i]);
        //workerthread[i].detach();
    }
    thread histograms[h];
    cout << "Creating Histogram Threads" << endl;
    for (int i = 0; i < h; ++i) {
       histograms[i]  = thread(histogram_thread_function, response_buffer, mVal, &hc);
       //histograms[i].detach();
    }

	/* Join all threads here */
    if (filename == "") {
        cout << "Finishing Data Then Joining Data Threads..." << endl;
    } else {
        cout << "Finishing File Then Joining File Thread..." << endl;
    }
    for (int i = 0; i < p; ++i) {
        patients[i].join();
    }
    //cout << "Patients done" << endl;

    for (int i = 0; i < w; ++i) {
        //cout << "Quitting" << endl;
        MESSAGE_TYPE m = QUIT_MSG;
        request_buffer->push((char*) &m, sizeof(MESSAGE_TYPE));
    }
    //cout << "quits sent" << endl;
    cout << "Finishing Work Then Joining Worker Threads..." << endl;
    for (int i = 0; i < w; ++i) {
        workerthread[i].join();
    }
    //cout << "workers joined" << endl;
    if (filename == "") {
        cout << "Finishing Histogram Then Joining Histogram Threads" << endl;
        for (int i = 0; i < h; ++i) {
            MESSAGE_TYPE m = QUIT_MSG;
            response_buffer->push((char*) &m, sizeof(MESSAGE_TYPE));
        }   
    }
    for (int i = 0; i < h; ++i) {
       histograms[i].join();
    }
    //cout << "hists joined" << endl;
    gettimeofday (&end, 0);
    // print the results
    if (filename == "") {
	    hc.print ();
    }
    int secs = (end.tv_sec * 1e6 + end.tv_usec - start.tv_sec * 1e6 - start.tv_usec)/(int) 1e6;
    int usecs = (int)(end.tv_sec * 1e6 + end.tv_usec - start.tv_sec * 1e6 - start.tv_usec)%((int) 1e6);
    cout << "Took " << secs << " seconds and " << usecs << " micro seconds" << endl;

    delete[] workers;   

    MESSAGE_TYPE q = QUIT_MSG;
    chan->cwrite ((char *) &q, sizeof (MESSAGE_TYPE));
    cout << "All Done!!!" << endl;
    delete chan; 
    delete response_buffer;
    delete request_buffer;
}
