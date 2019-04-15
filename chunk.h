/*
 * This file is provided under a dual BSD/GPLv2 license. When using or
 * redistributing this file, you may do so under either license.
 *
 * GPL LICENSE SUMMARY
 * Copyright(c) 2016 Baibantech Corporation.
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * BSD LICENSE
 * Copyright(c) 2016 Baibantech Corporation.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in
 * the documentation and/or other materials provided with the
 * distribution.
 * * Neither the name of Intel Corporation nor the names of its
 * contributors may be used to endorse or promote products derived
 * from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * sd-tree cluster vector and data block management
 *
 * Contact Information:
 * info-linux <info@baibantech.com.cn>
 */

#ifndef _SPLITTER_CLUSTER_H
#define _SPLITTER_CLUSTER_H

#include "spt_dep.h"
#include "spt_thread.h"
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

/* vec type*/
#define SPT_VEC_RIGHT 0
#define SPT_VEC_DATA 1

/* vec status */
#define SPT_VEC_VALID 0
#define SPT_VEC_INVALID 1
#define SPT_VEC_DB 3

/* vec scan status */
#define SPT_VEC_PVALUE  0
#define SPT_VEC_HVALUE 1
#define SPT_VEC_SCAN_LOCK 1

/* vec id bit len*/
#define SPT_VEC_BITS_LEN 21

/* vec nums in a cluster define by SPT_VEC_BITS_LEN*/
#define VEC_PER_CLUSTER  0x200000ull

/* vec grp info*/
#define VBLK_SIZE 8
#define VEC_BITS 3

#define GRP_SIZE 128	//16*8
#define GRP_BITS 7 

#define GRPS_PER_PG  32		//32
#define GRPS_PER_PG_BITS 5
#define GRPS_PER_PG_MASK 0x1F

#define VEC_PER_GRP 16 /*grp head alloc two entry ,so really 14*/
#define VEC_PER_GRP_BITS 4
#define VEC_PER_GRP_MASK 0x0F

#define DB_PER_GRP 7 /*db space is same as vec space but,db is two vec,so alloc map is half of vec*/

#define VEC_PER_PG (VEC_PER_GRP*GRPS_PER_PG)
#define GRP_ALLOCMAP_MASK 0xFFFFull
#define GRP_TICK_MASK 0xful
#define GRP_SPILL_START (96*1024)

/* cluster divide info*/

#define SPT_SORT_ARRAY_SIZE (4096*8)
#define SPT_DVD_CNT_PER_TIME (100)
#define SPT_DVD_THRESHOLD_VA (100000)
#define SPT_DVD_MOVE_TIMES (SPT_DVD_THRESHOLD_VA/(2*SPT_DVD_CNT_PER_TIME))
#define SPT_DATA_HIGH_WATER_MARK (150000)


/* data size and bit len*/
#define DATA_SIZE g_data_size
#define DATA_BIT_MAX (DATA_SIZE*8)


/* vec id to ptr address info */
#define PG_BITS 12
#define PG_SIZE (1<<PG_BITS)
#define CLST_PG_NUM_MAX ((VEC_PER_CLUSTER/VEC_PER_PG) + 1)
#define CLST_NDIR_PGS  (PG_SIZE - sizeof(struct cluster_head_t) - 3*sizeof(char*))/sizeof(char*)       //how much
#define CLST_IND_PG (CLST_NDIR_PGS)    //index
#define CLST_DIND_PG (CLST_IND_PG+1)    //index
#define CLST_N_PGS (CLST_DIND_PG+1)//pclst->pglist[] max

/*vec special value*/
#define SPT_NULL 0x1fffff
#define SPT_INVALID 0x1ffffe

/*scan direction*/
#define SPT_DIR_START 0
#define SPT_RIGHT 1
#define SPT_DOWN 2

/* data opt*/
#define SPT_OP_FIND 1
#define SPT_OP_DELETE 2
#define SPT_OP_INSERT 3
#define SPT_OP_DELETE_FIND 4

/* ret code */
#define CLT_FULL 3
#define CLT_NOMEM 2
#define CLT_ERR 1
#define SPT_OK 0
#define SPT_ERR -1
#define SPT_NOMEM -2
#define SPT_WAIT_AMT -3
#define SPT_DO_AGAIN -4
#define SPT_MASKED -5
#define SPT_NOT_FOUND 1

#define spt_set_vec_invalid(x) (x.status = SPT_VEC_INVALID)
#define spt_set_right_flag(x) (x.type = SPT_VEC_RIGHT)
#define spt_set_data_flag(x) (x.type = SPT_VEC_DATA)

#define spt_data_free_flag(x) ((x)->free)
#define spt_set_data_free_flag(x, y) ((x)->free = y)
#define spt_set_data_not_free(x) ((x)->free = 0)

#define SPT_HASH_BIT  11
#define SPT_HASH_MASK 0x3FF
#define SPT_POS_BIT 5
#define spt_get_pos_hash(x) ((x).pos >> SPT_POS_BIT)
#define spt_get_pos_offset(x) ((x).pos & 0x001F)


/*final process type*/

#define SPT_FIRST_SET 0
#define SPT_RD_UP 1
#define SPT_RD_DOWN 2
#define SPT_UP_DOWN 3
#define SPT_LAST_DOWN 4


#define THREAD_NUM_MAX 64

#define get_data_from_dh(x) ((char*)(void*)(long)((x)<<2))
#define set_data_to_dh(x) (((unsigned long long)(long)(void*)(x))>>2)

typedef char *(*spt_cb_get_key)(char *);
typedef void (*spt_cb_free)(char *);
typedef void (*spt_cb_end_key)(char *);
typedef char *(*spt_cb_construct)(char *);


struct spt_sort_info {
	int idx;
	int size;
	int cnt;
	char *array[0];//pdh->pdata
};


struct spt_dh_ref {
	volatile unsigned int hash;
	volatile int ref;
};
struct spt_dh {
	unsigned long long status  :1;
	unsigned long long free    :1;
	unsigned long long pdata   :62;
};

struct spt_vec {
	union {
		volatile unsigned long long val;
		struct {
			volatile unsigned long long status:      2;
			volatile unsigned long long type:       1;
			volatile unsigned long long scan_lock:   2;
			volatile unsigned long long	scan_status: 2; 
			volatile unsigned long long pos:        15;
			volatile unsigned long long down:       21;
			volatile unsigned long long rd:         21;
		};
	};
};

struct spt_grp
{
	union
	{
		volatile unsigned long long val;
		struct
		{
			volatile unsigned long long allocmap:		16;
			volatile unsigned long long freemap:		16;
		};
	};
	union {
		volatile unsigned long long control;
		struct {
			volatile unsigned long long tick:  4;
			volatile unsigned long long next_grp: 20;
			volatile unsigned long long pre_grp: 20;
			volatile unsigned long long resv:  20; 
		};
	};
};

struct cluster_address_trans_info {
	unsigned int pg_num_max;
	unsigned int pg_vec_num_total;
	unsigned int pg_db_num_total;
	unsigned int pg_ptr_bits;
};


struct spt_vec_debug {
	int window_id;
	int pre_vec_cnt;
	int hash_vec_cnt;
};

struct cluster_head_t {
	struct list_head c_list;
	int vec_head;
	int cluster_id;

	struct spt_vec *pstart;
	u64 startbit;
	u64 endbit;
	int is_bottom;
	int status;
	int ins_mask;
	unsigned int debug;
	volatile unsigned int data_total;

	unsigned int used_vec_cnt;
	unsigned int used_db_cnt;
	unsigned int spill_grp_id;
	unsigned int spill_db_grp_id;
	unsigned int thrd_total;
	unsigned int last_alloc_id;

	spt_cb_get_key get_key;
	spt_cb_get_key get_key_in_tree;
	spt_cb_free freedata;
	spt_cb_end_key get_key_end;
	spt_cb_end_key get_key_in_tree_end;
	spt_cb_construct construct_data;
	struct spt_vec_debug *vec_debug;
	char *cluster_vec_mem;
	char *cluster_db_mem;
	char *cluster_pos_mem;

	/* vec id to ptr info */
	struct cluster_address_trans_info address_info;
	volatile char **pglist_db;
	volatile char *pglist_vec[0];
};

struct spt_divided_info {
	struct cluster_head_t *pdst_clst;
	struct cluster_head_t *puclst;
	int divided_times;
	int down_is_bottom;
	char **up_vb_arr;
	char **down_vb_arr;
};


struct spt_dh_ext {
//struct dh_extent_t
	unsigned int hang_vec;
	struct cluster_head_t *plower_clst;
	char *data;
};


struct vec_cmpret_t {
	u64 smallfs;    /* small data firt set bit from the diff bit */
	u64 pos;        /* which bit is different */
	u32 finish;
};

struct query_info_t {
//spt_query_info
	 /* from which vector to start querying */
	struct spt_vec *pstart_vec;
	char *data;                 /* data to be queried */
	u64 endbit;                 /* data end bit */
	u32 startid;                /* start vector id */
	u32 startpos;
	u8 op;                      /* delete/find/insert */
	u8 data_type;               /* not used now */
	/* after deleting the data, whether it need to free by tree */
	u8 free_flag;
	/* return value,1 means >;0 means equal; -1 means < */
	char cmp_result;
	/* return value,the last compared data, when find return */
	u32 db_id;
	u32 ref_cnt;                /* return value, the duplicate count*/
	int multiple;               /* insert/delete data's count*/
	/* return value,the last compared vector, when find return */
	u32 vec_id;
	int originbit;
	/* if NULL, use the default callback function*/
	spt_cb_get_key get_key;
	/* if NULL, use the default callback function*/
	spt_cb_end_key get_key_end;
};

struct prediction_info_t {
	struct spt_vec *pstart_vec;
	char *data;                 /* data to be queried */
	u64 originbit;				/* first bit compare begin*/
	u64 endbit;                 /* data end bit */
	u32 startid;                /* start vector id */
	u32 ret_vec_id;             
	struct spt_vec *ret_vec;
	spt_cb_get_key get_key;
	spt_cb_end_key get_key_end;
};

struct insert_info_t {
	struct spt_vec *pkey_vec;
	u32 vec_real_pos;
	u64 key_val;
	u64 startbit;
	u64 fs;
	u64 cmp_pos;
	u64 endbit;         /* not include */
	u32 dataid;
	int ref_cnt;
	int key_id;
    u32 hang_vec;
	char *pcur_data; /*maped orig data*/
	char *pnew_data; /*maped new data*/
};

struct data_info_t {
	struct spt_vec cur_vec;
	struct spt_vec *pcur;
	struct spt_vec *pnext;
	int next_pos;
	int cur_vecid;
	u32 cur_data_id;
	char *pnew_data;
	char *pcur_data;
	u64 startbit;
	u64 fs;
	u64 cmp_pos;
	u64 endbit;         /* not include */
};

struct spt_stack {
	void **p_top;
	void **p_bottom;
	int stack_size;
};

struct spt_vec_f {
//spt_vec_full_t
	int down;
	int right;
	int data;
	long long pos;
};

struct travl_info {
//spt_traversal_info_st
	struct spt_vec_f vec_f;
	long long signpost;
};

static inline struct spt_grp *get_grp_from_grpid(struct cluster_head_t *pclst, unsigned int grp_id)
{
	char *page = pclst->cluster_vec_mem + ((grp_id >> GRPS_PER_PG_BITS) << PG_BITS); 
	return page + ((grp_id & GRPS_PER_PG_MASK) << GRP_BITS);
}
static inline struct spt_grp *get_db_grp_from_grpid(struct cluster_head_t *pclst, unsigned int grp_id)
{
	char *page = pclst->cluster_db_mem + ((grp_id >> GRPS_PER_PG_BITS) << PG_BITS); 
	return page + ((grp_id & GRPS_PER_PG_MASK) << GRP_BITS);
}

void vec_free(struct cluster_head_t *pcluster, int id);
void vec_list_free(struct cluster_head_t *pcluster, int id);
void db_free(struct cluster_head_t *pcluster, int id);

int spt_get_errno(void);
extern struct cluster_head_t *pgclst;

unsigned int db_alloc(struct cluster_head_t *pclst,
		struct spt_dh **db, struct spt_dh_ref **ref, unsigned int db_id);

struct cluster_head_t *cluster_init(int is_bottom,
						u64 startbit,
						u64 endbit,
						int thread_num,
						spt_cb_get_key pf,
						spt_cb_end_key pf2,
						spt_cb_free pf_free,
						spt_cb_construct pf_con);


void cluster_vec_add_page(struct cluster_head_t *pclst, int pg_id);
void cluster_db_add_page(struct cluster_head_t *pclst, int pg_id);

void cluster_destroy(struct cluster_head_t *pclst);
void free_data(char *p);
void default_end_get_key(char *p);

void debug_data_print(char *pdata);
extern int g_data_size;

char *insert_data(struct cluster_head_t *pclst, char *pdata);
char *delete_data(struct cluster_head_t *pclst, char *pdata);
char *insert_data_prediction(struct cluster_head_t *pclst, char *pdata);
char *delete_data_prediction(struct cluster_head_t *pclst, char *pdata);
char *insert_data_entry(struct cluster_head_t *pclst, char *pdata);
char *delete_data_entry(struct cluster_head_t *pclst, char *pdata);
void set_data_size(int size);

struct cluster_head_t *spt_cluster_init(u64 startbit,
						u64 endbit,
						int thread_num,
						spt_cb_get_key pf,
						spt_cb_end_key pf2,
						spt_cb_free pf_free,
						spt_cb_construct pf_con);


struct spt_thrd_t *spt_thread_init(int thread_num);
void spt_set_thrd_id(int val);

void vec_free(struct cluster_head_t *pcluster, int id);
void vec_list_free(struct cluster_head_t *pcluster, int id);
void db_free(struct cluster_head_t *pcluster, int id);

int spt_get_errno(void);
extern struct cluster_head_t *pgclst;

unsigned int db_alloc(struct cluster_head_t *pclst,
		struct spt_dh **db, struct spt_dh_ref **ref, unsigned int db_id);

struct cluster_head_t *cluster_init(int is_bottom,
						u64 startbit,
						u64 endbit,
						int thread_num,
						spt_cb_get_key pf,
						spt_cb_end_key pf2,
						spt_cb_free pf_free,
						spt_cb_construct pf_con);


void cluster_vec_add_page(struct cluster_head_t *pclst, int pg_id);
void cluster_db_add_page(struct cluster_head_t *pclst, int pg_id);

void cluster_destroy(struct cluster_head_t *pclst);
void free_data(char *p);
void default_end_get_key(char *p);

void debug_data_print(char *pdata);
extern int g_data_size;

char *insert_data(struct cluster_head_t *pclst, char *pdata);
char *delete_data(struct cluster_head_t *pclst, char *pdata);
char *insert_data_prediction(struct cluster_head_t *pclst, char *pdata);
char *delete_data_prediction(struct cluster_head_t *pclst, char *pdata);
char *insert_data_entry(struct cluster_head_t *pclst, char *pdata);
char *delete_data_entry(struct cluster_head_t *pclst, char *pdata);
void set_data_size(int size);

struct cluster_head_t *spt_cluster_init(u64 startbit,
						u64 endbit,
						int thread_num,
						spt_cb_get_key pf,
						spt_cb_end_key pf2,
						spt_cb_free pf_free,
						spt_cb_construct pf_con);


struct spt_thrd_t *spt_thread_init(int thread_num);
void spt_set_thrd_id(int val);

int spt_thread_start(int thread);
void spt_thread_exit(int thread);
int spt_divided_scan(struct cluster_head_t *pclst);

int debug_statistic(struct cluster_head_t *pclst);
void debug_cluster_travl(struct cluster_head_t *pclst);
void debug_lower_cluster_info_show(void);
int get_grp_by_data(struct cluster_head_t *pclst, char *data, int pos);

struct spt_pg_h *get_vec_pg_head(struct cluster_head_t *pclst, unsigned int pgid);
struct spt_pg_h *get_db_pg_head(struct cluster_head_t *pclst, unsigned int pgid);
char  *db_grp_id_2_ptr(struct cluster_head_t *pclst, unsigned int grp_id);
char  *db_ref_grp_id_2_ptr(struct cluster_head_t *pclst, unsigned int grp_id);
char  *vec_grp_id_2_ptr(struct cluster_head_t *pclst, unsigned int grp_id);

char* vec_id_2_ptr(struct cluster_head_t * pclst,unsigned int id);
char* db_id_2_ptr(struct cluster_head_t * pclst,unsigned int id);
char* db_ref_id_2_ptr(struct cluster_head_t * pclst,unsigned int id);

int db_alloc_from_grp(struct cluster_head_t *pclst, int id, struct spt_dh **db);

int vec_alloc(struct cluster_head_t *pclst, struct spt_vec **vec, unsigned int sed);
extern unsigned long find_next_bit(const unsigned long *addr, unsigned long size,
			    unsigned long offset);

int delete_next_vec(struct cluster_head_t *pclst,
		struct spt_vec next_vec,
		struct spt_vec *pnext,
		struct spt_vec cur_vec,
		struct spt_vec *pcur,
		int startbit,
		int direction );
struct spt_vec *replace_precise_vec(struct cluster_head_t *pclst,
		struct spt_vec *precise_vec,
		unsigned int seg_hash,
		int *vec_id);

#endif

