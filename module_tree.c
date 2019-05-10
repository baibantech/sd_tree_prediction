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
#include "sdtree_perf_stat.h"
struct cluster_head_t *spt_module_cluster;
char *module_tree_data[16];
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
	
	for (i = 0; i < 16; i++) {
		pdh_ext = spt_malloc(sizeof (struct spt_dh_ext) + HASH_WINDOW_LEN);
		if (!pdh_ext) {
			cluster_destroy(spt_module_cluster);
		}
		pdh_ext->data = (char *)(pdh_ext + 1);

		pdh_ext->plower_clst = NULL;
		pdata = pdh_ext->data;
		module_tree_data[i] = pdata;
		if (i != 15)
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
	for (i = 0; i < 16; i++) {
		for (j = 0; j < 16 - i -1; j++) {
			pdata = module_tree_data[j];
			next_data = module_tree_data[j + 1];
			if (memcmp(pdata, next_data, HASH_WINDOW_LEN) > 0) {
				module_tree_data[j] = next_data;
				module_tree_data[j + 1] = pdata;
			}
		}
	}
	
	for (i = 0 ; i < 16; i++) {
		printf("module tree data %2d:  ", i);
		pdata = module_tree_data[i];
		for (j = 0; j < HASH_WINDOW_LEN; j++)
			printf("%2x ", pdata[j]);
		printf("\r\n");
	}
}

unsigned int module_tree_alloc_ok;
unsigned int module_tree_alloc_conf;

int get_vec_by_module_tree(struct cluster_head_t *vec_clst, char *pdata, int pos,
		struct spt_vec *cur_vec,
		struct spt_vec **ret_vec,
		unsigned int *window_hash, unsigned int *seg_hash)
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
	int j;
	struct cluster_head_t * pclst = spt_module_cluster;

	window_byte = cur_byte = pdata + (pos / 8);
	cur_pos_len = window_len = pos/8; 
	spt_trace("cur byte is %p\r\n", cur_byte);

	while (*window_byte != gramma_window_symbol) {
		window_len--;
		window_byte--;
	}
	if (window_len != 0)
		grama_seg_hash = djb_hash(pdata, window_len);
	else
		grama_seg_hash = 0x1234;

	window_byte++;
	spt_trace("window byte is %p\r\n", window_byte);

	while (((unsigned long)(void*)window_byte) <= ((unsigned long) (void*)cur_byte)){
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
		spt_trace("pdext_h is %p \r\n", pdext_h);
		module_data = pdext_h->data;
		spt_trace("module data  %p\r\n", module_data);
		for (j = 0; j < HASH_WINDOW_LEN; j++)
			spt_trace("%2x ", module_data[j]);

		spt_trace("window_byte is %p\r\n",window_byte);
		for (j = 0; j < HASH_WINDOW_LEN; j++)
			spt_trace("%2x ", window_byte[j]);
		spt_trace("\r\n");
		*window_hash = grama_seg_hash;
		*seg_hash = djb_hash_seg(module_data, grama_seg_hash, 8);	
		vecid = vec_judge_full_and_alloc(vec_clst, ret_vec, *seg_hash);
		if (vecid != -1) {
			module_tree_alloc_ok++;
			break;
		}

		if (window_byte == cur_byte) {	
			vecid = vec_alloc(vec_clst, ret_vec, *seg_hash);
			module_tree_alloc_conf++;
			break;
		}
		window_byte++;
	}
	return vecid;
}

int find_vec_from_module_hash(struct cluster_head_t *vec_clst, char *pdata,
		struct spt_vec **ret_vec, int pos)
{
	struct query_info_t qinfo;
	struct spt_dh *pdh;
	struct spt_dh_ext *pdext_h;
	struct spt_vec *phang_vec, *pvec, vec;
	char *cur_byte,*window_byte, *module_data;
	int window_num, window_len, cur_pos_len;
	unsigned int grama_seg_hash, window_hash, seg_hash;
	int vecid = -1;
	int i = 0, j, gid;
	int ret;
	struct spt_grp *grp;
	struct cluster_head_t * pclst = spt_module_cluster;

	window_byte = cur_byte = pdata + (pos / 8);
	cur_pos_len = window_len = pos/8; 
	spt_trace("cur byte is %p\r\n", cur_byte);

	if (*window_byte != gramma_window_symbol)
		spt_assert(0);

	if (window_len != 0)
		grama_seg_hash = djb_hash(pdata, window_len);
	else
		grama_seg_hash = 0x1234;

	while (i < 1) {
		window_byte++;
		i++;
		spt_trace("window byte is %p\r\n", window_byte);
		grama_seg_hash = djb_hash_seg(window_byte, grama_seg_hash, 1);
		memset(&qinfo, 0, sizeof(struct query_info_t));
		qinfo.op = SPT_OP_FIND;
		qinfo.pstart_vec = pclst->pstart;
		qinfo.startid = pclst->vec_head;
		qinfo.endbit = pclst->endbit;
		qinfo.data = window_byte;
		
		PERF_STAT_START(find_stable_tree);
		ret = find_data_from_stable_cluster(pclst, &qinfo);
		PERF_STAT_END(find_stable_tree);
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
		spt_trace("pdext_h is %p \r\n", pdext_h);
		module_data = pdext_h->data;
		spt_trace("module data  %p\r\n", module_data);
		for (j = 0; j < HASH_WINDOW_LEN; j++)
			spt_trace("%2x ", module_data[j]);

		spt_trace("window_byte is %p\r\n",window_byte);
		for (j = 0; j < HASH_WINDOW_LEN; j++)
			spt_trace("%2x ", window_byte[j]);
		spt_trace("\r\n");

		window_hash = grama_seg_hash;
		seg_hash = djb_hash_seg(module_data, grama_seg_hash, 8);

		/*find seg_hash grp*/
		gid = seg_hash % GRP_SPILL_START;
		grp = get_grp_from_grpid(vec_clst, gid);
		pvec = (char *)grp + sizeof(struct spt_vec)*2;
		PERF_STAT_START(scan_grp_vec);
		for (j = 0; j < VEC_PER_GRP - 2; j++, pvec++) {
			if ((pvec->scan_status == SPT_VEC_HVALUE) &&(pvec->status != SPT_VEC_INVALID)) {
				if (spt_get_pos_hash(*pvec) == (window_hash & SPT_HASH_MASK)) {
					*ret_vec = pvec;
					vecid =  (gid << VEC_PER_GRP_BITS) + 2 + j;
					if (vec_id_2_ptr(vec_clst, vecid)!= pvec) {
						printf("gid, %d,grp %p,  pvec %p, vecid %d, clst %p\r\n", gid, grp, pvec, vecid, vec_clst);
						spt_assert(0);
					}
					PERF_STAT_END(scan_grp_vec);
					return vecid;
						
				}
			}
		}
		PERF_STAT_END(scan_grp_vec);
	}
	return -1;
}

void test_get_vec_by_module_tree(char *pdata, int pos) 
{
	struct cluster_head_t *pnext_clst;
	struct query_info_t qinfo = {0};
	struct spt_dh *pdh;
	int ret = 0;
	struct spt_vec *vec;
	unsigned int window_hash, seg_hash;
	/*
	 *first look up in the top cluster.
	 *which next level cluster do the data belong.
	 */
	pnext_clst = find_next_cluster(pgclst, pdata);
	if (pnext_clst == NULL) {
		spt_set_errno(SPT_MASKED);
		return 0;
	}
			
	get_vec_by_module_tree(pnext_clst, pdata, pos, NULL, &vec, &window_hash, &seg_hash);
}

unsigned int find_start_vec_ok;
unsigned int find_start_vec_err;
unsigned int start_vec_right;
unsigned int start_vec_wrong;
char *test_find_data_start_vec(char *pdata)
{
	struct cluster_head_t *pnext_clst;
	struct query_info_t qinfo = {0};
	struct spt_dh *pdh;
	int ret = 0, vecid, first_chbit;
	struct spt_vec *vec = NULL;
	unsigned int window_hash, seg_hash;
	int startpos, cur_data;
	char *pcur_data;
	/*
	 *first look up in the top cluster.
	 *which next level cluster do the data belong.
	 */
	pnext_clst = find_next_cluster(pgclst, pdata);
	if (pnext_clst == NULL) {
		spt_set_errno(SPT_MASKED);
		return NULL;
	}
	PERF_STAT_START(find_vec_from_module);
	vecid = find_vec_from_module_hash(pnext_clst, pdata,
		&vec, 192*8);
	PERF_STAT_END(find_vec_from_module);

	if (vec) {
		int real_pos;
		startpos =  192*8 + spt_get_pos_offset(*vec); 
		real_pos = get_real_pos_record(pnext_clst, vec);	
		if (startpos != real_pos) {
			//printf("pnext_clst is %p, vec %p, vecid %d, startpos%d, realpos %d\r\n", pnext_clst, vec, vecid,
			//		startpos, real_pos);
			find_start_vec_err++;
			return NULL;
		}

		
		cur_data = get_data_id (pnext_clst, vec, startpos);

		if (cur_data >= 0 && cur_data < SPT_INVALID) {
			find_start_vec_ok++;
			pdh = (struct spt_dh *)db_id_2_ptr(pnext_clst,
				cur_data);
			pcur_data = pnext_clst->get_key_in_tree(get_data_from_dh(pdh->pdata));
			
			first_chbit = get_first_change_bit(pdata,
					pcur_data,
					0,
					startpos);
			
			if (first_chbit != -1) {
				start_vec_wrong++;
				return pcur_data;
			} else {
				start_vec_right++;
				return NULL;
			}

		}
	}
	find_start_vec_err++;



	return NULL;	
}
