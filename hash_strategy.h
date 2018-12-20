/*************************************************************************
	> File Name: hash_strategy.h
	> Author: lijiyong0303
	> Mail: lijiyong0303@163.com 
	> Created Time: Thu 02 Aug 2018 05:45:23 PM CST
 ************************************************************************/
#ifndef __HASH_STRATEGY_H__
#define __HASH_STRATEGY_H__

#define HASH_WINDOW_LEN  4

#define HASH_MODE_SEEK 1
#define HASH_MODE_END 2

#define HASH_RECORD_WINDOW 4
struct hash_calc_result {
	struct hash_calc_result *next;
	int window_id;
	int result;
};

struct hash_calc_proc {
	int alloc_id;
	int max_item;
	struct hash_calc_result *next;
	struct hash_calc_result hresult[0];
};

struct hash_window_state {
	char *pdata;
	unsigned int real_pos;
};

struct precise_pos_record {
	int cur_index;
	int pos_array[0];
};

void calc_hash(char *data, unsigned int *window_hash,
		unsigned int *seg_hash, int pos);

void calc_hash_by_base(char *data,
		unsigned int base_hash,
		int base_pos, unsigned int *window_hash,
		unsigned int *seg_hash, int pos);

int get_real_pos_start(struct spt_vec *pvec);

int get_real_pos_next(struct spt_vec *pvec);

void real_pos_back(struct spt_vec *pvec,
		struct spt_vec *pre_vec);

int set_real_pos(struct spt_vec *pvec,
		unsigned int real_pos,
		unsigned int pre_pos,
		unsigned int real_hash);

int is_need_chg_pos(struct spt_vec *vec,
		struct spt_vec *next_vec,
		int type);

int find_start_vec(struct cluster_head_t *pclst,
		struct spt_vec **vec,
		int *start_pos,
		char *pdata,
		int window,
		char **ret_data);

#endif
