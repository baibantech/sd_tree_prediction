/*************************************************************************
	> File Name: hash_strategy.h
	> Author: lijiyong0303
	> Mail: lijiyong0303@163.com 
	> Created Time: Thu 02 Aug 2018 05:45:23 PM CST
 ************************************************************************/
#ifndef __HASH_STRATEGY_H__
#define __HASH_STRATEGY_H__

#define HASH_WINDOW_LEN  4

void calc_hash(char *data, unsigned int *window_hash,
		unsigned int *seg_hash, int pos);

void calc_hash_by_base(char *data,
		unsigned int base_hash,
		int base_pos, unsigned int *window_hash,
		unsigned int *seg_hash, int pos);

int get_real_pos_start(struct spt_vec *pvec);

int get_real_pos_next(struct spt_vec *pvec);

int roll_pos_back(struct spt_vec cur_vec);
void add_real_pos_record(struct cluster_head_t *pclst, struct spt_vec *pvec, int pos);
int get_real_pos_record(struct cluster_head_t *pclst, struct spt_vec *pvec);
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
