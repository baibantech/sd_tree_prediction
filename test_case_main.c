/*************************************************************************
	> File Name: test_case_main.c
	> Author: ma6174
	> Mail: ma6174@163.com 
	> Created Time: Tue 21 Feb 2017 07:35:16 PM PST
 ************************************************************************/
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>

#include <string.h>
#include <pthread.h>
#include <sched.h>
#include "data_set.h"
#include "data_cache.h"
#include "chunk.h"
#include "splitter_adp.h"
#include "spt_dep.h"
#include "module_tree.h"
char test_case_name[64] = {'t','e','s','t','_','c','a','s','e'};

typedef void (*test_proc_pfn)(void *args);

void* test_insert_thread(void *arg);
void* test_find_thread(void *arg);
void* test_pre_insert_thread(void *arg);
void* test_pre_delete_thread(void *arg);
void* test_pre_insert_proc(void *arg);
void* test_pre_delete_proc(void *arg);
void* test_vec_delete_proc(void *arg);
void* test_find_proc(void *arg);

void* test_delete_thread(void *arg);
void* test_divid_thread(void *arg);
void* test_vec_delete_thread(void *arg);
extern int sd_perf_debug;
int sd_perf_debug_1= 0;
enum cmd_index
{
	SET_NAME,
	DATA_LEN ,
	DATA_NUM,
	RANDOM_SET,
	FILE_SIZE,
	MAP_START_ADDR,
	MAP_READ_START,
	MAP_READ_LEN,
	CACHE_UNIT_SIZE,
};

struct cmdline_dsc
{
	int cmd_id;
	char *cmd;
	char *dsc;
};

struct cmdline_dsc cmdline[] = 
{
	{.cmd_id = SET_NAME,.cmd = "set_name",.dsc = "data set name ,type string"},
	
	{.cmd_id = DATA_LEN,.cmd = "instance_size",.dsc = "data instance len,type value"},
	{.cmd_id = DATA_NUM,.cmd = "instance_num",.dsc = "data instance num,type value"},
	
	{.cmd_id = RANDOM_SET,.cmd = "random_set",.dsc = "1 for random data construct ,otherwise specfic data set files"},
	{.cmd_id = FILE_SIZE,.cmd = "data_set_file_size",.dsc = "per data set file len,type value"},

	{.cmd_id = MAP_START_ADDR ,.cmd = "map_start_addr",.dsc = "data set map to memory from file,type value"},
	{.cmd_id = MAP_READ_START,.cmd = "map_read_start",.dsc = "the start data instance maped to memory start from 0 to instance_num-1",},
	{.cmd_id = MAP_READ_LEN,.cmd = "map_read_len",.dsc = "the instance num maped to memory from 1 to instance_num"},

	{.cmd_id = CACHE_UNIT_SIZE,.cmd = "read_cache_unit_size",.dsc = "the unit len read from map to test"},
};

enum test_proc_type
{
	find_proc,
	insert_proc,
	delete_proc,
};

struct proc_type
{
	int type;
	test_proc_pfn pfn;
};



struct proc_type test_proc_array[] = 
{
	{.type = insert_proc,.pfn = test_pre_insert_proc},
	
	{.type = delete_proc,.pfn = test_pre_delete_proc},
};

#define get_cmd_id 0
#define get_cmd_value_begin 1
#define get_cmd_value 2
#define get_cmd_end 3

int test_break_debug(void)
{
	printf("test_break_debug");
	return 0;
}
void print_cmd_help_info(void)
{
	int i = 0;
	printf("\r\ncmd style is xxx=xxx deperated by one space\r\n");
	
	for(i = 0; i < sizeof(cmdline)/sizeof(struct cmdline_dsc); i++)
	{
		printf("%s : %s \r\n",cmdline[i].cmd,cmdline[i].dsc);
	}
	return ;
}


int  parse_cmdline(int argc,char *argv[])
{
	int i ,j, k ;
	int cmd_id = -1;
	int step = get_cmd_id;

	for(i = 1; i < argc ; i++){
		j = 0;
		if(1 == i)
		{
			if(argv[i][0] == '?')
			{
				print_cmd_help_info();
				return 1;
			}
		}
		step = get_cmd_id;
		while(argv[i][j] != '\0')
		{
			if(argv[i][j]!= ' '){
				switch(step)
				{
					case get_cmd_id:
					{
						for(k = 0; k < sizeof(cmdline)/sizeof(struct cmdline_dsc); k++)
						{
							if(strncmp(&argv[i][j],cmdline[k].cmd,strlen(cmdline[k].cmd))== 0)
							{
								cmd_id = cmdline[k].cmd_id;
								j += strlen(cmdline[k].cmd);
								step = get_cmd_value_begin;
								break;
							}
						}
						if(-1 == cmd_id){
							goto error_cmd;
						}
						break;
					}
					
					case get_cmd_value_begin:
					{
						if(argv[i][j] != '=')
						{
							goto error_cmd;	
						}
						else
						{
							step = get_cmd_value;
							j++;
						}
						break;
					}

					case get_cmd_value:
					{
						long long value = -1;
						
						switch (cmd_id)
						{
							case SET_NAME:
							{
								strncpy(test_case_name,&argv[i][j],64);
								printf(test_case_name);
								break;
							}
							
							case DATA_LEN:
							{
								value = strtol(&argv[i][j],NULL,10);
								printf("value is %lld\r\n",value);
								set_instance_len_config(value);
								break;
							}

							case DATA_NUM :
							{
								value = strtol(&argv[i][j],NULL,10);
								printf("value is %lld\r\n",value);
								set_instance_num_config(value);
								break;
							}
							case RANDOM_SET:
							{
								value = strtol(&argv[i][j],NULL,10);
								printf("value is %lld\r\n",value);
								set_instance_random(value);
								break;
							}
							
							case FILE_SIZE:
							{
								value = strtol(&argv[i][j],NULL,10);
								printf("value is %lld\r\n",value);
								set_data_file_len_config(value);
								break;
							}

							case MAP_START_ADDR:
							{
								value = strtol(&argv[i][j],NULL,10);
								printf("value is %lld\r\n",value);
								set_data_map_start_addr(value);
								break;
							}
							case MAP_READ_START:
							{
								value = strtol(&argv[i][j],NULL,10);
								printf("value is %lld\r\n",value);
								set_map_read_start(value);
								break;
							}
							case MAP_READ_LEN:
							{
								value = strtol(&argv[i][j],NULL,10);
								printf("value is %lld\r\n",value);
								set_map_read_len(value);
								break;
							}
						
							case CACHE_UNIT_SIZE:
							{
								value = strtol(&argv[i][j],NULL,10);
								printf("value is %lld\r\n",value);
								set_read_cache_unit_size(value);
								break;
							}	
							default :
							{
								printf("error cmd id in get value\r\n");
								goto error_cmd;
							}
						
						}
						step = get_cmd_end;

						break;
					}
					
					case get_cmd_end:
					{
						j++;
						break;
					}

					default :
						printf("error parse cmd step\r\n");
						goto error_cmd;
				}
			}
			else
			{
				j++;
			}
		}
	}
	
	return 0;

error_cmd:
	printf("error cmdline %s\r\n",argv[i]);
	return -1;
}
extern int vec_hash_cnt;
extern int module_tree_hash_one_byte;
extern int module_tree_alloc_ok;
extern int module_tree_alloc_conf;
extern int module_tree_hash_node;
extern int module_tree_hash_branch;
extern int module_tree_hash_preseg_err;
extern int vec_chg_pos_ptov;
extern int vec_chg_pos_vtop;
extern int hash_stat_switch;
struct data_set_file *set_file_list = NULL;
extern int test_find_data_by_vec;
extern int delete_data_from_root;;
int main(int argc,char *argv[])
{
	int ret = -1;
	void *addr = NULL;
	long long get_data_len;
	int i = 0 ;
	int err;
	pthread_t ntid;
	int thread_num = 0;
	int cnt = 0;
	if(parse_cmdline(argc,argv))
	{
		return 0;
	}
	set_file_list = get_data_set_file_list();

	if(NULL == set_file_list)
	{
		printf("get file list error\r\n");
		return 0;
	}
	ret = construct_data_set(set_file_list);
	printf("construct_data_set ret is %d\r\n",ret);
	
	data_set_config_map_address = 0x7fa0746e000;
	
	addr  = map_data_set_file_anonymous(set_file_list,0x7fa0746e000);
	//printf("map data set file ret is %d\r\n",ret);
	if(-1 == data_set_config_map_read_start)
	{
		data_set_config_map_read_start = 0;
	}

	if(-1 == data_set_config_map_read_len )
	{
		get_data_len = -1;
	}
	else
	{
		get_data_len = data_set_config_map_read_len*data_set_config_instance_len;
	}
	get_data_from_file(set_file_list,data_set_config_map_read_start*data_set_config_instance_len,get_data_len);
	
	//set data size
	
	set_data_size(data_set_config_instance_len);
	
	//init cluster head
	thread_num = data_set_config_insert_thread_num + data_set_config_delete_thread_num + 1;	
    printf("thread_num is %d\r\n",thread_num);

    sd_perf_stat_init();
	hash_stat_init();
	pgclst = spt_cluster_init(0,DATA_BIT_MAX, 12, 
                              tree_get_key_from_data,
                              tree_free_key,
                              tree_free_data,
                              tree_construct_data_from_key);
    if(pgclst == NULL)
    {
        spt_debug("cluster_init err\r\n");
        return 1;
    }
	spt_module_tree_init(64, module_cluster_data_total); 

    g_thrd_h = spt_thread_init(thread_num);
    if(g_thrd_h == NULL)
    {
        spt_debug("spt_thread_init err\r\n");
        return 1;
	}

#if 1
	err = pthread_create(&ntid, NULL, test_divid_thread, 1);
	if (err != 0)
		printf("can't create thread: %s\n", strerror(err));
#endif

	g_thrd_id = 0;
	err = pthread_create(&ntid, NULL, test_insert_thread, 3);
	if (err != 0)
		printf("can't create thread: %s\n", strerror(err));
	sleep(60);
	hash_stat_switch = 1;
	err = pthread_create(&ntid, NULL, test_delete_thread, 2);
	if (err != 0)
		printf("can't create thread: %s\n", strerror(err));


	sleep(15);
	vec_hash_cnt = 0;
	module_tree_hash_one_byte = 0;
	module_tree_alloc_ok = 0;
	module_tree_alloc_conf = 0;
	module_tree_hash_node = 0;
	module_tree_hash_branch = 0;
	module_tree_hash_preseg_err = 0;
	vec_chg_pos_ptov = 0;
	vec_chg_pos_vtop = 0;
	spt_cluster_scan_mem_init(pgclst);
	err = pthread_create(&ntid, NULL, test_insert_thread, 3);
	if (err != 0)
		printf("can't create thread: %s\n", strerror(err));

#if 0
	sleep(30);
	test_find_data_by_vec = 1;
	err = pthread_create(&ntid, NULL, test_find_thread, 1);
	if (err != 0)
		printf("can't create thread: %s\n", strerror(err));
	
	sleep(30);
	test_find_data_by_vec = 0;
	err = pthread_create(&ntid, NULL, test_find_thread, 2);
	if (err != 0)
		printf("can't create thread: %s\n", strerror(err));
#endif
#if 0
	err = pthread_create(&ntid, NULL, test_insert_thread, 3);
	if (err != 0)
		printf("can't create thread: %s\n", strerror(err));
	err = pthread_create(&ntid, NULL, test_delete_thread, 4);
	if (err != 0)
		printf("can't create thread: %s\n", strerror(err));
	sleep(30);
#endif
	delete_data_from_root = 0;
	err = pthread_create(&ntid, NULL, test_vec_delete_thread, 2);
	if (err != 0)
		printf("can't create thread: %s\n", strerror(err));
	while(1)
	{
		sleep(1);
	}
	return 0;

}
extern int total_data_num;
void *test_insert_data(char *pdata)
{
	int bit_len;
	bit_len = get_string_bit_len(pdata, 0);	
	total_data_num++;
	return insert_data(pgclst, pdata, bit_len);
}
void *test_delete_data(char *pdata)
{
	int bit_len;
	char *data;
	bit_len = get_string_bit_len(pdata, 0);	
	PERF_STAT_START(whole_delete);	
	data = delete_data(pgclst, pdata, bit_len);
	PERF_STAT_END(whole_delete);
	return data;
}

void test_find_cluster(char *pdata)
{
	struct cluster_head_t *pclst;
	int bit_len;
	bit_len = get_string_bit_len(pdata, 0);	
	pclst = find_next_cluster(pgclst, pdata, bit_len);
	printf("cur cluster id %d\r\n", pclst->cluster_id);
	sleep(1);
}
extern char *find_data_by_hash(struct cluster_head_t *pclst, char *pdata, int data_bit_len);
extern char *query_data(struct cluster_head_t *pclst, char *pdata, int data_bit_len);
void *test_find_data(char *pdata, int bit_len)
{
	return find_data_by_hash(pgclst, pdata, bit_len);
	//return query_data(pgclst, pdata);
}
void* test_find_thread(void *arg)
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
	local_pre_seg_hash = 0;
	local_bottom_clst = NULL;

	test_find_proc(i);
	if(i != 0)
	{	
		while(1)
		{
			sleep(10);		
		}
	}
}

int test_stop = 1;
void* test_insert_thread(void *arg)
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
	//while(test_stop == 0)
	    test_pre_insert_proc(i);
	while(1)
	{
		sleep(10);		
	}
}

void* test_vec_delete_thread(void *arg)
{
	int i = (long)arg;
	cpu_set_t mask;
	g_thrd_id = i;
	CPU_ZERO(&mask);
	CPU_SET(i,&mask);
	printf("delete thead cpu %d, %p\r\n", i, arg);
	if(sched_setaffinity(0,sizeof(mask),&mask)== -1)
	{
		printf("warning: could not set CPU AFFINITY\r\n");
	}
	//while(test_stop == 0)
	test_vec_delete_proc(i);
	while(1)
	{
		sleep(1);
	}
}
void* test_delete_thread(void *arg)
{
	int i = (long)arg;
	cpu_set_t mask;
	g_thrd_id = i;
	CPU_ZERO(&mask);
	CPU_SET(i,&mask);
	printf("delete thead cpu %d, %p\r\n", i, arg);
	if(sched_setaffinity(0,sizeof(mask),&mask)== -1)
	{
		printf("warning: could not set CPU AFFINITY\r\n");
	}
	//while(test_stop == 0)
	    test_pre_delete_proc(i);
	while(1)
	{
		sleep(1);
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
void *test_scan_thread(void *arg);

void test_scan_proc(void)
{
	int err, ntid;
	err = pthread_create(&ntid, NULL, test_scan_thread, (void *)2);
	if (err != 0)
		printf("can't create thread: %s\n", strerror(err));

}
extern int spt_cluster_scan(struct cluster_head_t *pclst);
void *test_scan_thread(void *arg)
{
	int i = (long)arg;
	cpu_set_t mask;
	g_thrd_id = i;
	CPU_ZERO(&mask);
	CPU_SET(i,&mask);
	if(sched_setaffinity(0,sizeof(mask),&mask)== -1)
		printf("warning: could not set CPU AFFINITY\r\n");
	
	sleep(10);
#if 1
	spt_cluster_scan(pgclst);
#else
	data_rb_tree_scan();
#endif
	while (1)
		sleep(1);
}


void test_delete_data_thread(int cpu)
{
	int err, ntid;
	err = pthread_create(&ntid, NULL, test_delete_thread, cpu);
	if (err != 0)
		printf("can't create thread: %s\n", strerror(err));

}
