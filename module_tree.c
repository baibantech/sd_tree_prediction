/*************************************************************************
	> File Name: module_tree.c
	> Author: lijiyong0303
	> Mail: lijiyong0303@163.com 
	> Created Time: Tue 16 Apr 2019 05:01:37 PM CST
 ************************************************************************/

#include <stdio.h>
#include "chunk.h"
#include "spt_dep.h"
#include "splitter.h"
#include "hash_strategy.h"
struct cluster_head_t *spt_module_cluster;
char *module_tree_data[64];
struct cluster_head_t * spt_module_tree_init(u64 startbit, u64 endbit, int thread_num,
			spt_cb_get_key  pf,
			spt_cb_end_key  pf2,
			spt_cb_free pf_free,
			spt_cb_construct pf_con)
{
	struct cluster_head_t *pclst, *plower_clst;
	struct spt_dh_ext *pdh_ext;
	char *pdata;
	int i;
	
	pclst = spt_module_cluster = cluster_init(0, 0, HASH_WINDOW_LEN*8,
			thread_num, pf, pf2, free_data,
				NULL);

	if (pclst == NULL)
		return NULL;
	INIT_LIST_HEAD(&pclst->c_list);
	pclst->cluster_id = 0xffff;
	
	for (i = 0; i < 64; i++) {
		pdh_ext = spt_malloc(sizeof (struct spt_dh_ext) + HASH_WINDOW_LEN);
		if (!pdh_ext) {
			cluster_destroy(spt_module_cluster);
		}
		pdh_ext->data = (char *)(pdh_ext + 1);

		pdh_ext->plower_clst = NULL;
		pdata = pdh_ext->data;
		module_tree_data[i] = pdata;
		if (i != 63)
			get_random_string(pdata, HASH_WINDOW_LEN);
		else 
			memset(pdata, 0xFF, HASH_WINDOW_LEN);

		do_insert_data(spt_module_cluster, (char *)pdh_ext,
				spt_module_cluster->get_key_in_tree,
				spt_module_cluster->get_key_in_tree_end);

	}
	return spt_module_cluster;	
}

void show_module_tree_data(void)
{
	int i, j;
	char *pdata, *next_data;
	for (i = 0; i < 64; i++) {
		for (j = 0; j < 64 - i -1; j++) {
			pdata = module_tree_data[j];
			next_data = module_tree_data[j + 1];
			if (memcmp(pdata, next_data, HASH_WINDOW_LEN) > 0) {
				module_tree_data[j] = next_data;
				module_tree_data[j + 1] = pdata;
			}
		}
	}
	
	for (i = 0 ; i < 64; i++) {
		printf("module tree data %2d:  ", i);
		pdata = module_tree_data[i];
		for (j = 0; j < HASH_WINDOW_LEN; j++)
			printf("%2x ", pdata[j]);
		printf("\r\n");
	}
}


int get_vec_by_module_tree(struct cluster_head_t *pclst, char *pdata, int pos, struct spt_vec **ret_vec, unsigned int *window_hash, unsigned int *seg_hash)
{
	struct query_info_t qinfo;
	int ret;
	struct spt_dh *pdh;
	struct spt_dh_ext *pdext_h;
	struct spt_vec *phang_vec, *pvec, vec;
	char *cur_byte,*window_byte, *module_data;
	int window_num, window_len, cur_pos_len;
	unsigned int grama_seg_hash;
	int vecid = -1;
	
	cur_byte = pdata + (pos / 8);
	cur_pos_len = window_len = pos/8; 
	while (*cur_byte != gramma_window_symbol) {
		window_len--;
		window_byte--;
	}
	if (window_len != 0)
		grama_seg_hash = djb_hash(pdata, window_len);
	else
		grama_seg_hash = 0x1234;

	window_byte++;
	while (((unsigned long)(void*)window_byte) <= ((unsigned long) (void*)cur_byte)){
		printf("window_byte is %p\r\n",window_byte);
		grama_seg_hash = djb_hash_seg(window_byte, grama_seg_hash, 1);
		memset(&qinfo, 0, sizeof(struct query_info_t));
		qinfo.op = SPT_OP_FIND;
		qinfo.pstart_vec = pclst->pstart;
		qinfo.startid = pclst->vec_head;
		qinfo.endbit = pclst->endbit;
		qinfo.data = window_byte;

		ret = find_data_from_stable_cluster(pclst, &qinfo);
		if (ret >= 0) {
			pdh = (struct spt_dh *)db_id_2_ptr(pclst, qinfo.db_id);
			pdext_h = (struct spt_dh_ext *)get_data_from_dh(pdh->pdata);
			
			if (qinfo.cmp_result < 0) {
				/* < 0 continue to look down */
				pvec = (struct spt_vec *)vec_id_2_ptr(pclst,
						qinfo.vec_id);
				vec.val = pvec->val;
				/* difference appear in leaf node*/
				if (vec.type != SPT_VEC_DATA) {
					pvec = (struct spt_vec *)vec_id_2_ptr(pclst, vec.rd);
					ret = find_lowest_data(pclst, pvec);
					if (ret == SPT_NULL)
						spt_assert(0);
					pdh = (struct spt_dh *)db_id_2_ptr(pclst, ret);
					pdext_h = (struct spt_dh_ext *)get_data_from_dh(pdh->pdata);
				}

			} else if (qinfo.cmp_result > 0){
				/* > 0 continue to look from hang vec*/
				if (pdext_h->hang_vec >= SPT_INVALID)
					spt_assert(0);
				phang_vec = (struct spt_vec *)vec_id_2_ptr(pclst,
						pdext_h->hang_vec);
				vec.val = phang_vec->val;
				if (vec.type == SPT_VEC_DATA) {
					if (vec.rd == SPT_NULL)
						spt_assert(0);
					pdh = (struct spt_dh *)db_id_2_ptr(pclst,
							vec.rd);
					pdext_h = (struct spt_dh_ext *)get_data_from_dh(pdh->pdata);

				} else {
					pvec = (struct spt_vec *)vec_id_2_ptr(pclst, vec.rd);
					ret = find_lowest_data(pclst, pvec);
					if (ret == SPT_NULL)
						spt_assert(0);
					pdh = (struct spt_dh *)db_id_2_ptr(pclst, ret);
					pdext_h = (struct spt_dh_ext *)get_data_from_dh(pdh->pdata);
				}

			}
		} else {
			spt_debug("find_data err!¥r¥n");
			spt_assert(0);
		}
		module_data = pdext_h->data;
		printf("module data  %p\r\n", module_data);
		*window_hash = grama_seg_hash;
		*seg_hash = djb_hash_seg(module_data, grama_seg_hash, 8);	
		if (window_byte != cur_byte) {
			vecid = vec_judge_full_and_alloc(pclst, ret_vec, *seg_hash);
			if (vecid != -1)
				break;
			window_byte++;
		} else {
			vecid = vec_alloc(pclst, ret_vec, grama_seg_hash);
		}
	}
	return vecid;
}


void test_get_vec_by_module_tree(char *pdata, int pos) 
{




}
