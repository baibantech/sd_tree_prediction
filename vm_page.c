/*************************************************************************
	> File Name: test_case_main.c
	> Author: ma6174
	> Mail: ma6174@163.com 
	> Created Time: Tue 21 Feb 2017 07:35:16 PM PST
 ************************************************************************/
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <pthread.h>
#include <sched.h>
#include "chunk.h"
#include "splitter_adp.h"
#include "spt_dep.h"
void* test_insert_thread(void *arg);
void* test_delete_thread(void *arg);
void* test_divid_thread(void *arg);

long data_len = 0;
char *data_begin = NULL;;
long get_file_size(char *file_name)
{
	struct stat statbuf;
	stat(file_name,&statbuf);
	return statbuf.st_size;
}
char *file_name= "mem_test0.bin";
int main(int argc,char *argv[])
{
	int err;
	int i =0;
	pthread_t ntid;
	int thread_num = 0;
	int fd ;
	long r_cnt;
	long read_size;
	int read_unit = 10*1024*1024;
	long file_size = 0;
	void *map_addr;
	printf("file_name is %s\r\n",file_name);
	file_size =get_file_size(file_name);
	printf("file_size is %lld\r\n",file_size);
	file_size = ((file_size +4096)/4096)*4096;
	map_addr = mmap(NULL ,file_size ,PROT_READ|PROT_WRITE,MAP_ANONYMOUS|MAP_PRIVATE,-1,0);
	if(-1 == map_addr) {
		return -1;
	}
	data_begin = map_addr;
	data_len = file_size;
	madvise(map_addr,file_size+4096,MADV_MERGEABLE);
	printf("map_addr is %p\r\n",map_addr);
	
	fd = open(file_name,O_RDWR|O_DIRECT);
//	fd = open(file_name,O_RDWR);
	if(-1 == fd)
	{
		printf("err in get data file \r\n");	
	}
	read_size = file_size;
	i =0;
	while(read_size >= read_unit)
	{
		r_cnt =read(fd,map_addr+i*read_unit,read_unit);
		if(r_cnt == -1)
		{
			perror("read:");
		}
		i++;
		read_size-=read_unit;
		if(r_cnt != read_unit){
			printf("error read file cnt is %lx\r\n",r_cnt);
		}
	}
	if(read_size)
		r_cnt =read(fd,map_addr+i*read_unit,read_size);

	close(fd);
	printf("r_cnt is %lx\r\n",r_cnt);


	set_data_size(4096);
	
	thread_num = 3;	
    printf("thread_num is %d\r\n",thread_num);

	pgclst = spt_cluster_init(0,DATA_BIT_MAX, thread_num, 
                              tree_get_key_from_data,
                              tree_free_key,
                              tree_free_data,
                              tree_construct_data_from_key);
    if(pgclst == NULL)
    {
        spt_debug("cluster_init err\r\n");
        return 1;
    }

    g_thrd_h = spt_thread_init(thread_num);
    if(g_thrd_h == NULL)
    {
        spt_debug("spt_thread_init err\r\n");
        return 1;
	}
	err = pthread_create(&ntid, NULL, test_divid_thread, (void *)thread_num-1);
	if (err != 0)
		printf("can't create thread: %s\n", strerror(err));

	g_thrd_id = 1;
	
	test_insert_thread(1);
	printf("insert over\r\n");

	while(1)
	{
		sleep(10);
	}

	return 0;
}
long insert_data_total = 0;
char *insert_data_ptr ;
void* test_insert_thread(void *arg)
{
	int i = (long)arg;
	cpu_set_t mask;
	g_thrd_id = i;
	char *pdata;
	long cnt = 0;
	CPU_ZERO(&mask);
	CPU_SET(i,&mask);
	if(sched_setaffinity(0,sizeof(mask),&mask)== -1)
	{
		printf("warning: could not set CPU AFFINITY\r\n");
	}
	
	while(cnt *4096 < data_len){
		pdata = data_begin + cnt*4096;
		insert_data_total++;
		insert_data_ptr = pdata;
		insert_data_prediction(pgclst, pdata);
		cnt++;
	}

}
void *test_divid_thread(void *arg)
{
	int i = (long)arg;
	cpu_set_t mask;
	g_thrd_id = i;
	CPU_ZERO(&mask);
	CPU_SET(i,&mask);
	if(sched_setaffinity(0,sizeof(mask),&mask)== -1)
	{
		printf("warning: could not set CPU AFFINITY\r\n");
	}
	
	while(1)
	{
		sleep(1);
		spt_divided_scan(pgclst);
	}
}
