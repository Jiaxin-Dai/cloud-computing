#include <iostream>

#include <string>
#include <fcntl.h>
#include <cstdlib>
#include <unistd.h>
#include <string>
#include <ctime>
#include <random>
#include <thread>
#include <cstring>
#include <iomanip>
#include <ratio>
#include <chrono>
#include <functional>

using std::vector;using std::thread;
using std::string;using std::cin;using std::cout;
using std::endl;
using namespace std::chrono;

class Bench {
public:


    Bench()= default;
    static double SequentialWrite(long long fileSize_,long long recordSize_,string filename)
    {

        auto start=std::chrono::steady_clock::now();
        int fd;//file descriptor
        const char* cfilename=filename.c_str();

        //with O_SYNC flag,After this open() call, every write() to the file
        // automatically flushes the file data and
        //metadata to the disk
        //O_EXCL With O_CREAT : create file exclusively
        //O_RDWR: Open for reading and writing
        //O_CREAT:Create file if it doesn’t already exist
        //O_DIRECT:File I/O bypasses buffer cache
        //O_TRUNC,Truncate existing file to zero length
        fd = open(cfilename, O_DIRECT|O_TRUNC | O_RDWR | O_CREAT , S_IRUSR | S_IWUSR);
        if(fd==-1)
        {
            cout<<"open file failed in SequentialWrite "<<endl;
            exit(1);
        }

        //because one block of the disk is 4096Bytes
        // so if we want to write 4097 bytes , we need a 8192 buffer, which is exactly 2*4096Bytes
        //((recordSize_+4095)&(-4096)) did this job
        long long recordS=((recordSize_+4095)&(-4096));


        //cout << "rounded record size " << recordS << endl;
        char* buffer=(char*)aligned_alloc(4096,recordS* sizeof(char));// 4KB write buffer
        memset(buffer,'A',recordS* sizeof(char));

        //time_t start, end;//timer
        //auto start=std::chrono::steady_clock::now();
        long long tempS=recordS;

        while(tempS < fileSize_)//Write 4K every time
        {
            //int rt = write(fd, buffer, recordS*sizeof(char));//
            int rt = write(fd, buffer, recordS);
            if(rt == -1)
            {
                cout << "error during writing into file in SequentialWrite"<<endl;
                exit(1);
            }
            tempS += recordS;
        }
        // auto end=std::chrono::steady_clock::now();;//end time

        //fsync( fd ); //no need we have O_SYNC
        close(fd);
        //runtime in seconds
        // double runTime=std::chrono::duration<double>(end-start).count();

        // double throughput=(fileSize_/runTime)/1000000;

        // cout << " sequential write time in seconds " <<  std::fixed <<runTime << std::setprecision(5) << endl;
        // cout << "sequential writet throughput : " << throughput << endl;
        free(buffer);
        return 1;
    }
    static double SequentialRead(long long fileSize_,long long recordSize_,string filename)
    {
        int fd;//file descriptor
        const char* cfilename=filename.c_str();

        //with O_SYNC flag,After this open() call, every write() to the file
        // automatically flushes the file data and
        //metadata to the disk
        //O_EXCL With O_CREAT : create file exclusively
        //O_RDWR: Open for reading and writing
        //O_CREAT:Create file if it doesn’t already exist
        //O_DIRECT:File I/O bypasses buffer cache
        //O_TRUNC,Truncate existing file to zero length
        fd = open(cfilename, O_DIRECT|O_RDONLY);
        if(fd==-1)
        {
            cout<<"open file failed in SequentialRead "<<endl;
            exit(1);
        }
        long long recordS=((recordSize_+4095)&(-4096));
        cout << "rounded record size " << recordS << endl;
        char* buffer=(char*)aligned_alloc(4096,recordS* sizeof(char));// 4KB write buffer

        //time_t start, end;//timer

        long long tempS=recordS;

        //time(&start);//start time
        while(tempS < fileSize_)//Read 4K every time
        {
            //int rt = write(fd, buffer, recordS*sizeof(char));//
            int rt = read(fd, buffer, recordS);
            if(rt == -1)
            {
                cout << "error during reading file in SequentialRead"<<endl;
                exit(1);
            }
            tempS += recordS;
        }
        //time(&end);//end time

        /* fsync(int fd );*/ //no need we have O_SYNC
        close(fd);
        //size_t runTime= end-start;

        //double throughput=(fileSize_/runTime)/1000000;

        //cout << " sequential read time in seconds " <<  std::fixed <<runTime << std::setprecision(5) << endl;
        //cout << "sequential read throughput : " << throughput << endl;
        free(buffer);
        return 1;
    }

    static double RandomWrite(long long fileSize_,long long recordSize_,string filename)
    {   //find a block randomly every time, note the file must exit before in to this function
        int fd;//file descriptor
        const char* cfilename=filename.c_str();

        //with O_SYNC flag,After this open() call, every write() to the file
        // automatically flushes the file data and
        //metadata to the disk
        //O_EXCL With O_CREAT : create file exclusively
        //O_RDWR: Open for reading and writing
        //O_CREAT:Create file if it doesn’t already exist
        //O_DIRECT:File I/O bypasses buffer cache
        //O_TRUNC,Truncate existing file to zero length
        fd = open(cfilename,   O_RDWR|O_DIRECT, S_IRUSR | S_IWUSR);
        if(fd==-1)
        {
            cout<<"open file failed in RandomWrite "<<endl;
            exit(1);
        }
        long long recordS=((recordSize_+4095)&(-4096));;

        //cout << "rounded record size " << recordS << endl;
        char* buffer=(char*)aligned_alloc(4096,recordS* sizeof(char));// 4KB write buffer
        memset(buffer,'B',recordS* sizeof(char));

        //time_t start, end;//timer

        long long tempS=recordS;

        //time(&start);//start time
        long long randN=0;
        long long fileLessOneBlock=fileSize_-recordS;
        int ioperation=0;// number of operation
        while(tempS < fileSize_)//Write 4K every time
        {
            //int rt = write(fd, buffer, recordS*sizeof(char));//
            randN=rand()%fileLessOneBlock;// genenrate a number between[0,filesize-one block];
            int pos = ((randN + 4095) & (-4096));
            lseek(fd,pos,SEEK_SET); // move the offset from the beginning
            int rt = write(fd, buffer, recordS);
            if(rt == -1)
            {
                cout << "error during writing into file in RandomWrite"<<endl;
                exit(1);
            }
            tempS += recordS;
            ioperation++;
            //fsync( fd );// //no need we have O_SYNC
        }
        //time(&end);//end time


        close(fd);
        //size_t runTime= end-start;

        //double throughput=(fileSize_/runTime)/1000000;

        //cout << " Random write time in seconds " <<  std::fixed <<runTime << std::setprecision(5) << endl;
        //cout << "Random writet throughput : " << throughput << endl;
        cout<<"RW OP"<<ioperation<<endl;
        free(buffer);
        return ioperation;
    }


    static double RandomRead(long long fileSize_,long long recordSize_,string filename)
    {
//find a block randomly every time, note the file must exit before in to this function
        int fd;//file descriptor
        const char* cfilename=filename.c_str();

        //with O_SYNC flag,After this open() call, every write() to the file
        // automatically flushes the file data and
        //metadata to the disk
        //O_EXCL With O_CREAT : create file exclusively
        //O_RDWR: Open for reading and writing
        //O_CREAT:Create file if it doesn’t already exist
        //O_DIRECT:File I/O bypasses buffer cache
        //O_TRUNC,Truncate existing file to zero length
        fd = open(cfilename,   O_DIRECT|O_RDWR  );
        if(fd==-1)
        {
            cout<<"open file failed in RandomRead "<<endl;
            exit(1);
        }
        long long recordS=((recordSize_+4095)&(-4096));
        //cout << "rounded record size " << recordS << endl;
        char* buffer=(char*)aligned_alloc(4096,recordS* sizeof(char));// 4KB read buffer
        memset(buffer,'C',recordS* sizeof(char));

        //time_t start, end;//timer

        long long tempS=recordS;

        //time(&start);//start time

        long long randN=0;
        long long fileLessOneBlock=fileSize_-recordS;
        int ioperation=0;// number of operation
        while(tempS < fileSize_)//Read 4K every time
        {
            //int rt = write(fd, buffer, recordS*sizeof(char));//
            randN=rand()%fileLessOneBlock;// genenrate a number between[0,filesize-one block];
            int pos = ((randN + 4095) & (-4096));
            lseek(fd,pos,SEEK_SET); // move the offset from the beginning
            int rt = read(fd, buffer, recordS);
            if(rt == -1)
            {
                cout << "error during reading into file in RandomRead"<<endl;
                exit(1);
            }
            tempS += recordS;
            ioperation++;
        }
        //time(&end);//end time

        /* fsync(int fd );*/ //no need we have O_SYNC
        close(fd);
        //size_t runTime= end-start;

        //double throughput=(fileSize_/runTime)/1000000;

        //cout << " Random read time in seconds " <<  std::fixed <<runTime << std::setprecision(5) << endl;
        //cout << "Random read throughput : " << throughput << endl;
        cout<<"RR OP"<<ioperation<<endl;

        free(buffer);
        return ioperation;
    }



    //std::vector<thread> threads;
};

static long GB=1073741824;
static long MB=1048576;
static long KB=1024;
long long str2bytes(string str)
{
    //find unit
    char unit=str[str.length()-1];
    long long numofBytes=0;

    numofBytes= std::stoll(str.substr(0,str.length()-1));
    switch(unit){
        case 'k':
            numofBytes*=KB;
            break;
        case 'm':
            numofBytes*=MB;
            break;
        case 'g':
            numofBytes*=GB;
            break;
        default:
            throw std::invalid_argument("str2bytes failed!");
            break;
    }

    return numofBytes;
}
// print workload info
string workload2str(int wkint)
{
    string wkstr;
    switch(wkint) {
        case 0:
            wkstr = "Sequential write";
            break;
        case 1:
            wkstr = "Sequential read";
            break;
        case 2:
            wkstr = "Random write";
            break;
        case 3:
            wkstr = "Random read";
            break;
        default:
            throw std::invalid_argument("str2bytes failed!");
            break;
    }
    return wkstr;
}


int main(int argc,char* argv[]){


    try {


        if (argc < 9) {
            cout << "Arguments too few! exit!" << endl;
            return -1;
        }
        string cmd, arg;
        int numthreads=1;
        int workload=0;
        auto fileSize=0ll;
        auto recordSize=0ll;
        //Parse input arguments here
        for (int iarg = 1; iarg < argc - 1; ++iarg) {
            cmd=(argv[iarg]);
            iarg++;
            arg=(argv[iarg]);
            if (cmd=="-t") {//how many threads
                numthreads=std::stoi(arg);

            } else if (cmd=="-s") {// file Size
                fileSize=str2bytes(arg);

            } else if (cmd=="-r"){// record Size
                recordSize=str2bytes(arg);
            } else if (cmd=="-i"){
                workload=std::stoi(arg);
            }else{
                cout<<"unable to parse input "<<cmd<<" exit!"<<endl;
                return -1;
            }
        }
        //main thread creat other threads and  calculate time

        long long numBlocks = fileSize / 4096;//////////////////////////////

        cout<<"Input Summary:"<<endl
            <<"Number of Threads: "<<numthreads<<endl
            <<"File Size: "<<fileSize<<endl
            <<"Record Size: "<<recordSize<<endl
            << "WorkLoad:"<<workload2str(workload)<<endl;

        vector<std::thread> threads(numthreads);
        string testfile = "dummy";

        //timer

        long long filesizeThread = fileSize/numthreads;
        auto start=std::chrono::steady_clock::now();//start time

       // Bench::SequentialWrite(fileSize,recordSize,testfile);
        for (int ithread = 0; ithread < numthreads; ++ithread) {


            switch(workload)
            {
                case 0://Sequential write
                    threads[ithread] = std::thread(Bench::SequentialWrite, filesizeThread, recordSize,
                                                   testfile + std::to_string(ithread));
                    break;
                case 1://Sequential read
                    threads[ithread] = std::thread(Bench::SequentialRead, filesizeThread, recordSize,
                                                   testfile + std::to_string(ithread));
                    break;
                case 2://Random write
                    threads[ithread] = std::thread(Bench::RandomWrite, filesizeThread, recordSize,
                                                   testfile + std::to_string(ithread));
                    break;
                case 3://Random read
                    threads[ithread] = std::thread(Bench::RandomRead, filesizeThread, recordSize,
                                                   testfile + std::to_string(ithread));
                    break;
                default:
                    break;
            }



        }
        for(int ithread = 0; ithread < numthreads; ++ithread)
        {
            threads[ithread].join();// main thread wait other threads all finish their jobs here
        }

        auto end=std::chrono::steady_clock::now();;//endtime
        //runtime in seconds
        double runTime=std::chrono::duration<double>(end-start).count();

        double throughput=(fileSize*numthreads/runTime)/MB;\
        cout << " All time in seconds " <<  std::fixed <<runTime << std::setprecision(5) << endl;
        cout<<"throughput in MB/Sec "<<throughput<<endl;

    }catch (std::exception &e){
        cout<<e.what()<<endl;
    }
	return 0;
}
