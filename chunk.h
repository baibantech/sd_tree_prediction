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
#define SPT_VEC_RIGHT 0
#define SPT_VEC_DATA 1
#define SPT_VEC_SIGNPOST 2
#define SPT_VEC_SYS_FLAG_DATA 1


#define SPT_VEC_VALID 0
#define SPT_VEC_INVALID 1
#define SPT_VEC_RAW 2


#define SPT_VEC_SIGNPOST_BIT 15

#define SPT_VEC_SIGNPOST_MASK ((1ul<<SPT_VEC_SIGNPOST_BIT)-1)


#define spt_set_vec_invalid(x) (x.status = SPT_VEC_INVALID)
#define spt_set_right_flag(x) (x.type = SPT_VEC_RIGHT)
#define spt_set_data_flag(x) (x.type = SPT_VEC_DATA)

#define SPT_PTR_MASK (0x00000000fffffffful)
#define SPT_PTR_VEC (0ul)
#define SPT_PTR_DATA (1ul)

#define SPT_BUF_TICK_BITS 18
#define SPT_BUF_TICK_MASK ((1<<SPT_BUF_TICK_BITS)-1)

#define SPT_PER_THRD_RSV_CNT 5
#define SPT_BUF_VEC_WATERMARK 200
#define SPT_BUF_DATA_WATERMARK 100

#define spt_data_free_flag(x) ((x)->rsv&0x1)
#define spt_set_data_free_flag(x, y) ((x)->rsv |= y)
#define spt_set_data_not_free(x) ((x)->rsv &= 0xfffe)


#define SPT_SORT_ARRAY_SIZE (4096*8)
#define SPT_DVD_CNT_PER_TIME (100)
#define SPT_DVD_THRESHOLD_VA (1000000)
#define SPT_DVD_MOVE_TIMES (SPT_DVD_THRESHOLD_VA/(2*SPT_DVD_CNT_PER_TIME))
#define SPT_DATA_HIGH_WATER_MARK (1600000)

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

struct spt_buf_list {
//    unsigned long long flag:1;
	unsigned long long tick:18;
	unsigned long long id:23;
	unsigned long long next:23;
};

struct block_head_t {
	unsigned int magic;
	unsigned int next;
};

struct db_head_t {
	unsigned int magic;
	unsigned int next;
};

struct spt_dh {
//spt_data_hd
	volatile int ref;
	u16 size;
	u16 rsv;
	char *pdata;
};

struct spt_vec {
	union {
		volatile unsigned long long val;
		struct {
			volatile unsigned long long status:      2;
			volatile unsigned long long type:       1;
			volatile unsigned long long pos:        15;
			volatile unsigned long long down:       23;
			volatile unsigned long long rd:         23;
		};
		struct {
			volatile unsigned long long dummy_flag:         3;
			volatile unsigned long long ext_sys_flg:        6;
			volatile unsigned long long ext_usr_flg:        6;
			volatile unsigned long long idx:                24;
			volatile long long dummy_rd:           23;
		};
	};
};

struct cluster_head_t {
	struct list_head c_list;
	int vec_head;

	struct spt_vec *pstart;
	u64 startbit;
	u64 endbit;
	int is_bottom;
	volatile unsigned int data_total;

	unsigned int pg_num_max;
	unsigned int pg_num_total;
	unsigned int pg_cursor;
	unsigned int pg_ptr_bits;

	unsigned int free_vec_cnt;
	unsigned int used_vec_cnt;
	unsigned int used_db_cnt;
	unsigned int thrd_total;
	unsigned int last_alloc_id;

	int status;
	int ins_mask;
	unsigned int debug;
	spt_cb_get_key get_key;
	spt_cb_get_key get_key_in_tree;
	//void (*freedata)(char *p, u8 flag);
	spt_cb_free freedata;
	spt_cb_end_key get_key_end;
	spt_cb_end_key get_key_in_tree_end;
	spt_cb_construct construct_data;
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


struct vec_head_t {
	unsigned int magic;
	unsigned int next;
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
	u64 signpost;               /* not used now */
	char *data;                 /* data to be queried */
	u64 endbit;                 /* data end bit */
	u32 startid;                /* start vector id */
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
	int res;
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
//spt_insert_info
	struct spt_vec *pkey_vec;
	u64 key_val;
	u64 signpost;
	u64 startbit;
	u64 fs;
	u64 cmp_pos;
	u64 endbit;         /* not include */
	u32 dataid;
	int ref_cnt;
	u32 key_id;
    u32 hang_vec;
	/* for debug */
	int alloc_type;
	char *pcur_data;
	struct vec_cmpret_t cmpres;
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
//#define DBLK_BITS 3
#define DATA_SIZE g_data_size
#define RSV_SIZE 2
#define DBLK_SIZE (sizeof(struct spt_dh))
#define VBLK_BITS 3
#define VBLK_SIZE (1<<VBLK_BITS)
#define DATA_BIT_MAX (DATA_SIZE*8)

//#define vec_id_2_ptr(pchk, id)    ((char *)pchk+id*VBLK_SIZE);

unsigned int vec_alloc(struct cluster_head_t *pclst, struct spt_vec **vec);
void vec_free(struct cluster_head_t *pcluster, int id);
void vec_list_free(struct cluster_head_t *pcluster, int id);
void db_free(struct cluster_head_t *pcluster, int id);

int spt_get_errno(void);
extern struct cluster_head_t *pgclst;

//DECLARE_PER_CPU(u32,local_thrd_id);
//DECLARE_PER_CPU(int,local_thrd_errno);
//DECLARE_PER_CPU(int,process_enter_check);
//#define g_thrd_id     per_cpu(local_thrd_id,smp_processor_id())
//#define g_thrd_errno  per_cpu(local_thrd_errno,smp_processor_id())



#define CLST_PG_NUM_MAX ((1<<14)) /*cluster max = 64m*/
//#define CHUNK_SIZE (1<<16)
#define PG_BITS 12
#define PG_SIZE (1<<PG_BITS)
#define BLK_BITS 5
#define BLK_SIZE (1<<BLK_BITS)

#define CLST_NDIR_PGS 320        //how much

#define CLST_IND_PG (CLST_NDIR_PGS)    //index
#define CLST_DIND_PG (CLST_IND_PG+1)    //index
#define CLST_N_PGS (CLST_DIND_PG+1)//pclst->pglist[] max


//#define CLST_TIND_PGS        (CLST_DIND_PGS+1)


//extern char* blk_id_2_ptr(cluster_head_t *pclst, unsigned int id);
//extern char* db_id_2_ptr(cluster_head_t *pclst, unsigned int id);
//extern char* vec_id_2_ptr(cluster_head_t *pclst, unsigned int id);

#define SPT_NULL 0x7fffff
#define SPT_INVALID 0x7ffffe

#define SPT_DIR_START 0
#define SPT_RIGHT 1
#define SPT_DOWN 2

#define SPT_OP_FIND 1
#define SPT_OP_DELETE 2
#define SPT_OP_INSERT 3


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

unsigned int db_alloc(struct cluster_head_t *pclst,
		struct spt_dh **db);

struct cluster_head_t *cluster_init(int is_bottom,
						u64 startbit,
						u64 endbit,
						int thread_num,
						spt_cb_get_key pf,
						spt_cb_end_key pf2,
						spt_cb_free pf_free,
						spt_cb_construct pf_con);

void cluster_destroy(struct cluster_head_t *pclst);
void free_data(char *p);
void default_end_get_key(char *p);
void vec_buf_free(struct cluster_head_t *pclst, int thread_id);
void db_buf_free(struct cluster_head_t *pclst, int thread_id);


void debug_data_print(char *pdata);
extern int g_data_size;

char *insert_data(struct cluster_head_t *pclst, char *pdata);
char *delete_data(struct cluster_head_t *pclst, char *pdata);
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

#define SPT_TOP_INSERT 0
#define SPT_USER_INSERT 2

struct spt_grp
{
	union
	{
		volatile unsigned long long val;
		struct
		{
			volatile unsigned long long allocmap:		30;
			volatile unsigned long long freemap:		30;
			volatile unsigned long long tick:			4;
		};
	};
};

 struct spt_pg_h
{
	unsigned int bit_used;
};

#define GRP_SIZE 248	//64+30*8
#define GRPS_PER_PG  (PG_SIZE/GRP_SIZE)		//16
#define VEC_PER_GRP 30
#define GRP_ALLOCMAP_MASK 0x3FFFFFFFull
#define PG_HEAD_OFFSET (GRP_SIZE*GRPS_PER_PG)
#define PG_SPILL_WATER_MARK 360

#define GRP_TICK_MASK 0xful
struct spt_pg_h *get_vec_pg_head(struct cluster_head_t *pclst, unsigned int pgid);
struct spt_pg_h *get_db_pg_head(struct cluster_head_t *pclst, unsigned int pgid);
char  *db_grp_id_2_ptr(struct cluster_head_t *pclst, unsigned int grp_id);
char  *vec_grp_id_2_ptr(struct cluster_head_t *pclst, unsigned int grp_id);

char* vec_id_2_ptr(struct cluster_head_t * pclst,unsigned int id);
char* db_id_2_ptr(struct cluster_head_t * pclst,unsigned int id);

int vec_alloc_from_grp(struct cluster_head_t *pclst, int id, struct spt_vec **vec,int line);
int db_alloc_from_grp(struct cluster_head_t *pclst, int id, struct spt_dh **db);
extern unsigned long find_next_bit(const unsigned long *addr, unsigned long size,
			    unsigned long offset);

#endif

