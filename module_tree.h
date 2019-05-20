/*************************************************************************
	> File Name: module_tree.h
	> Author: lijiyong0303
	> Mail: lijiyong0303@163.com 
	> Created Time: Tue 16 Apr 2019 05:01:42 PM CST
 ************************************************************************/
#ifndef MODULE_TREE_H__
#define MODULE_TREE_H__
#include  <stdio.h>

#define module_cluster_data_total (16)
#define SPT_MODULE_NULL  (module_cluster_data_total * 2 + 1)
#define SPT_VEC_MODULE_INVALID module_cluster_data_total*2 
#define SPT_DB_MODULE_INVALID  module_cluster_data_total
struct module_cluster_head_t {
	int vec_head;
	struct spt_module_vec *pstart;
	u64 startbit;
	u64 endbit;
	unsigned int data_total;
	unsigned int max_data_total;
	unsigned int used_vec_cnt;
	unsigned int last_alloc_id;
	char *vec_mem;
};

struct spt_module_vec {
	union {
		volatile unsigned long long val;
		struct {
			volatile unsigned long long status:      1;
			volatile unsigned long long type:        1;
			volatile unsigned long long pos:        16;
			volatile unsigned long long down:       10;
			volatile unsigned long long rd:         10;
		};
	};
};


struct spt_module_dh_ext {
	unsigned int hang_vec;
	int dataid;
	char *data;
};
struct module_query_info_t {
//spt_query_info
	 /* from which vector to start querying */
	struct spt_module_vec *pstart_vec;
	char *data;                 /* data to be queried */
	u64 endbit;                 /* data end bit */
	u32 startid;                /* start vector id */
	u32 startpos;
	u8 op;                      /* delete/find/insert */
	/* return value,1 means >;0 means equal; -1 means < */
	char cmp_result;
	/* return value,the last compared data, when find return */
	u32 db_id;
	u32 vec_id;
};

struct module_insert_info_t {
	struct spt_module_vec *pkey_vec;
	u64 key_val;
	u64 startbit;
	u64 fs;
	u64 cmp_pos;
	u64 endbit;         /* not include */
	u32 dataid;
	int key_id;
    u32 hang_vec;
};

struct module_data_info_t {
	struct spt_module_vec cur_vec;
	struct spt_module_vec *pcur;
	struct spt_module_vec *pnext;
	int cur_vecid;
	u32 cur_data_id;
	u64 startbit;
	u64 fs;
	u64 cmp_pos;
	u64 endbit;         /* not include */
};
struct module_cluster_head_t *module_cluster_init(u64 endbit, int data_total);
int get_module_data_id(struct module_cluster_head_t *pclst,
		struct spt_module_vec *pvec);
int final_module_vec_process(struct module_cluster_head_t *pclst,
		struct module_query_info_t *pqinfo ,
		struct module_data_info_t *pdinfo, int type); 
int find_data_from_module_cluster(struct module_cluster_head_t *pclst,
		struct module_query_info_t *pqinfo);
#endif
