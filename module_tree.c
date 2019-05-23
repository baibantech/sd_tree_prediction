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
#include "module_tree.h"
#include "sdtree_perf_stat.h"

struct module_cluster_head_t *spt_module_cluster;
struct spt_module_dh_ext *module_array_data[512];
int  vec_module_alloc(struct module_cluster_head_t *pm_cluster, struct spt_module_vec **vec)
{
	int vecid = atomic_add_return(1, (atomic_t *)&pm_cluster->last_alloc_id);
	if (vecid >= SPT_VEC_MODULE_INVALID)
		spt_assert(0);

	*vec = pm_cluster->vec_mem + vecid *sizeof(struct spt_module_vec);
	return vecid;
}
void  vec_module_free(struct module_cluster_head_t *pm_cluster, int vecid){
	spt_assert(0);
}


struct module_cluster_head_t * module_cluster_init(u64 endbit, int data_total)
{
	struct module_cluster_head_t *pm_cluster;
	struct spt_module_vec *start_vec;

	if (data_total > module_cluster_data_total) {
		printf("module cluster data may too high\r\n");
		return NULL;
	}

	pm_cluster = spt_malloc(sizeof(struct module_cluster_head_t));
	if (!pm_cluster)
		return NULL;

	pm_cluster->startbit = 0;
	pm_cluster->endbit = endbit;
	pm_cluster->max_data_total = data_total;
	pm_cluster->vec_mem = spt_malloc(sizeof(struct spt_module_vec)*data_total*2 + (sizeof(struct spt_module_dh_ext) + HASH_WINDOW_LEN)*data_total);
	pm_cluster->db_mem = pm_cluster->vec_mem + sizeof(struct spt_module_vec)*data_total*2;
	pm_cluster->vec_head = 0;
	start_vec = pm_cluster->pstart = (struct spt_module_vec *)pm_cluster->vec_mem;
	
	start_vec->val = 0;
	start_vec->pos = -1;
	start_vec->type = SPT_VEC_DATA;
	start_vec->down = SPT_MODULE_NULL;
	start_vec->rd = SPT_MODULE_NULL;
		
	pm_cluster->last_alloc_id = 1;

	return pm_cluster;
}

struct spt_module_vec * vec_module_id_2_ptr(struct module_cluster_head_t *pm_clst, int vecid)
{
	spt_assert(vecid < SPT_VEC_MODULE_INVALID);
	return pm_clst->vec_mem + vecid *sizeof(struct spt_module_vec);
}

char *db_module_id_2_ptr(struct module_cluster_head_t *pm_clst, int db_id)
{
	return pm_clst->db_mem + (sizeof(struct spt_module_dh_ext) + HASH_WINDOW_LEN)*db_id;
}

int do_module_insert_first_set(struct module_cluster_head_t *pclst,
	struct module_insert_info_t *pinsert,
	char *new_encapdata)
{
	u32 dataid;
	struct spt_module_vec tmp_vec, *pcur;
	struct spt_module_dh_ext *pdh_ext;

	tmp_vec.val = pinsert->key_val;
	pdh_ext = (struct spt_module_dh_ext *)new_encapdata;
	dataid = pdh_ext->dataid;

	pcur = (struct spt_vec *)vec_module_id_2_ptr(pclst, pclst->vec_head);
	spt_assert(pcur == pinsert->pkey_vec);
	tmp_vec.rd = pdh_ext->dataid;

	pdh_ext->hang_vec = SPT_MODULE_NULL;
	
	smp_mb();/* ^^^ */
	if (pinsert->key_val == atomic_cmpxchg(
		(atomic_t *)pinsert->pkey_vec,
		pinsert->key_val, tmp_vec.val))
		return dataid;

	return SPT_DO_AGAIN;
}

int do_module_insert_up_via_r(struct module_cluster_head_t *pclst,
	struct module_insert_info_t *pinsert,
	char *new_encapdata)
{
	struct spt_module_vec tmp_vec, cur_vec, *pvec_a, *pvec_b;
	u32 dataid, vecid_a, vecid_b, tmp_rd;
	struct spt_module_dh_ext *pdh_ext, *plast_dh_ext;
	pvec_b = NULL;
	
	tmp_vec.val = pinsert->key_val;
	tmp_rd = tmp_vec.rd;
	pdh_ext = (struct spt_module_dh_ext *)new_encapdata;
	dataid = pdh_ext->dataid;

	vecid_a = vec_module_alloc(pclst, &pvec_a);
	if (!pvec_a) {
		spt_print("¥r¥n%d¥t%s", __LINE__, __func__);
		return SPT_NOMEM;
	}
	pvec_a->val = 0;
	pvec_a->type = SPT_VEC_DATA;
	pvec_a->pos = pinsert->cmp_pos - 1;
	pvec_a->rd = dataid;
	tmp_vec.rd = vecid_a;

	if (tmp_vec.pos == pvec_a->pos && tmp_vec.pos != 0)
			spt_assert(0);

	if (tmp_vec.type == SPT_VEC_DATA
		|| pinsert->endbit > pinsert->fs) {

		vecid_b = vec_module_alloc(pclst, &pvec_b);
		if (!pvec_b) {
			spt_print("¥r¥n%d¥t%s", __LINE__, __func__);
            vec_module_free(pclst, vecid_a);
			return SPT_NOMEM;
		}
		
		pvec_b->val = 0;
		pvec_b->type = tmp_vec.type;
		pvec_b->pos = pinsert->fs - 1;
		pvec_b->rd = tmp_rd;
		pvec_b->down = SPT_MODULE_NULL;
		tmp_vec.type = SPT_VEC_RIGHT;
		pvec_a->down = vecid_b;
	} else
		pvec_a->down = tmp_rd;
	
	plast_dh_ext = (struct spt_module_dh_ext *)db_module_id_2_ptr(pclst, pinsert->dataid);
	pdh_ext->hang_vec = plast_dh_ext->hang_vec;

	if (pinsert->key_val == atomic_cmpxchg(
		(atomic_t *)pinsert->pkey_vec,
		pinsert->key_val, tmp_vec.val)) {
		plast_dh_ext->hang_vec = vecid_a;
		return dataid;
	}

	vec_module_free(pclst, vecid_a);
	if (pvec_b != NULL)
    	vec_module_free(pclst, vecid_b);
	
	return SPT_DO_AGAIN;
}

int do_module_insert_down_via_r(struct module_cluster_head_t *pclst,
	struct module_insert_info_t *pinsert,
	char *new_encapdata)
{
	struct spt_module_vec tmp_vec, cur_vec, *pvec_a, *pvec_b;
	u32 dataid, vecid_a, vecid_b, tmp_rd;
	struct spt_module_dh_ext *pdh_ext;

	tmp_vec.val = pinsert->key_val;
	tmp_rd = tmp_vec.rd;
	pdh_ext = (struct spt_module_pdh_ext *)new_encapdata;
	dataid = pdh_ext->dataid;

	vecid_a = vec_module_alloc(pclst, &pvec_a);
	if (!pvec_a) {
		spt_print("¥r¥n%d¥t%s", __LINE__, __func__);
		return SPT_NOMEM;
	}
	pvec_a->val = 0;
	pvec_a->pos = pinsert->cmp_pos - 1;

	vecid_b = vec_module_alloc(pclst, &pvec_b);
	if (!pvec_b) {
		spt_print("¥r¥n%d¥t%s", __LINE__, __func__);
		vec_module_free(pclst, vecid_a);
		return SPT_NOMEM;
	}
	pvec_b->val = 0;
	pvec_b->type = SPT_VEC_DATA;
	pvec_b->pos = pinsert->fs - 1;
	pvec_b->rd = dataid;
	pvec_b->down = SPT_MODULE_NULL;

	if (tmp_vec.type == SPT_VEC_DATA) {
		pvec_a->type = SPT_VEC_DATA;
		pvec_a->rd = tmp_vec.rd;
		tmp_vec.type = SPT_VEC_RIGHT;
	} else {
		pvec_a->type = SPT_VEC_RIGHT;
		pvec_a->rd = tmp_vec.rd;
	}
	pvec_a->down = vecid_b;

	tmp_vec.rd = vecid_a;
	if (tmp_vec.pos == pvec_a->pos && tmp_vec.pos != 0)
			spt_assert(0);
	
	pdh_ext->hang_vec = vecid_a;
	
	if (pinsert->key_val == atomic_cmpxchg(
		(atomic_t *)pinsert->pkey_vec,
		pinsert->key_val, tmp_vec.val)) {
		pinsert->hang_vec = vecid_a;
		return dataid;
	}
    vec_module_free(pclst, vecid_a);
    vec_module_free(pclst, vecid_b);
	
	return SPT_DO_AGAIN;
}

int do_module_insert_last_down(struct module_cluster_head_t *pclst,
	struct module_insert_info_t *pinsert,
	char *new_encapdata)
{
	struct spt_module_vec tmp_vec, *pvec_a;
	u32 dataid, vecid_a;
	struct spt_module_dh_ext *pdh_ext;
	
	tmp_vec.val = pinsert->key_val;
	pdh_ext = (struct spt_module_ext *)new_encapdata;
	dataid = pdh_ext->dataid;

	vecid_a = vec_module_alloc(pclst, &pvec_a);
	if (!pvec_a) {
		spt_print("¥r¥n%d¥t%s", __LINE__, __func__);
		return SPT_NOMEM;
	}
	pvec_a->val = 0;
	pvec_a->type = SPT_VEC_DATA;
	pvec_a->pos = pinsert->fs - 1; 
	pvec_a->rd = dataid;
	pvec_a->down = SPT_MODULE_NULL;
	
	tmp_vec.down = vecid_a;
	pdh_ext->hang_vec = pinsert->key_id;

	if (pinsert->key_val == atomic_cmpxchg(
		(atomic_t *)pinsert->pkey_vec,
		pinsert->key_val, tmp_vec.val))
		return dataid;

    vec_module_free(pclst, vecid_a);
	return SPT_DO_AGAIN;
}
int do_module_insert_up_via_d(struct module_cluster_head_t *pclst,
	struct module_insert_info_t *pinsert,
	char *new_encapdata)
{
	struct spt_module_vec tmp_vec, cur_vec, *pvec_a, *pvec_down;
	u32 dataid, down_dataid, vecid_a, tmp_down;
	struct spt_module_dh_ext *pdh_ext;

	pdh_ext = (struct spt_module_dh_ext *)new_encapdata;
	dataid = pdh_ext->dataid;
	tmp_vec.val = pinsert->key_val;

	vecid_a = vec_module_alloc(pclst, &pvec_a);
	if (!pvec_a) {
		spt_print("¥r¥n%d¥t%s", __LINE__, __func__);
		return SPT_NOMEM;
	}
	pvec_a->val = 0;
	pvec_a->type = SPT_VEC_DATA;
	pvec_a->pos = pinsert->fs - 1;
	pvec_a->rd = dataid;

	pvec_a->down = tmp_vec.down;
	tmp_vec.down = vecid_a;

	pdh_ext->hang_vec = pinsert->key_id;
	
	if (pinsert->key_val == atomic_cmpxchg(
				(atomic_t *)pinsert->pkey_vec,
				pinsert->key_val, tmp_vec.val)) {
		

		pvec_down = (struct spt_vec *)vec_module_id_2_ptr(pclst,
			pvec_a->down);

		down_dataid = get_module_data_id(pclst, pvec_down);
		if (down_dataid < 0) {
			spt_debug("get_data_id error¥r¥n");
			spt_assert(0);
		}
		pdh_ext = (struct spt_module_dh_ext *)db_module_id_2_ptr(pclst, down_dataid);
		pdh_ext->hang_vec = vecid_a;
		return dataid;
	}

    vec_module_free(pclst, vecid_a);
	return SPT_DO_AGAIN;
}

int final_module_vec_process(struct module_cluster_head_t *pclst,
		struct module_query_info_t *pqinfo,
		struct module_data_info_t *pdinfo,
		int type) 
{
	struct module_insert_info_t st_insert_info = { 0};
	int cur_data = pdinfo->cur_data_id;
	int ret = SPT_NOT_FOUND;
	struct spt_module_dh_ext *pdh_ext = (void *)pqinfo->data;
	char *pnew_data = pdh_ext->data;
	
	switch (pqinfo->op) {
		case SPT_OP_FIND:
			if (cur_data == SPT_DB_MODULE_INVALID) {
				cur_data = get_module_data_id(pclst,pdinfo->pcur);
				if (cur_data >= 0
					&& cur_data < SPT_DB_MODULE_INVALID) {
				} else if (cur_data == SPT_DO_AGAIN)
					return SPT_DO_AGAIN;
				else 
					return cur_data;
			}

			switch (type) {
				case SPT_FIRST_SET:
					break;
				case SPT_RD_UP:
					pqinfo->db_id = cur_data;
					pqinfo->vec_id = pdinfo->cur_vecid;
					pqinfo->cmp_result = 1;
					break;
				case SPT_RD_DOWN:
				case SPT_LAST_DOWN:
					pqinfo->db_id = cur_data;
					pqinfo->vec_id = pdinfo->cur_vecid;
					pqinfo->cmp_result = -1;
					break;
				case SPT_UP_DOWN:
					cur_data = get_module_data_id(pclst, pdinfo->pcur);
					if (cur_data >= 0
						&& cur_data < SPT_DB_MODULE_INVALID) {
					} else if (cur_data == SPT_DO_AGAIN) {
						return SPT_DO_AGAIN;
					} else {
						return cur_data;
					}
					pqinfo->db_id = cur_data;
					pqinfo->vec_id = pdinfo->cur_vecid;
					pqinfo->cmp_result = -1;
					break;
				default : spt_assert(0);
					break;
			}	
			return ret;

		case SPT_OP_INSERT:
			st_insert_info.pkey_vec = pdinfo->pcur;
			st_insert_info.key_val = pdinfo->cur_vec.val;
			st_insert_info.cmp_pos = pdinfo->cmp_pos;	
			st_insert_info.key_id = pdinfo->cur_vecid;
			switch (type) {
				case SPT_FIRST_SET:
					ret = do_module_insert_first_set(
							pclst,
							&st_insert_info,
							pqinfo->data);
					
					if (ret == SPT_DO_AGAIN) {
						return SPT_DO_AGAIN;
					} else if (ret >= 0) {
						pqinfo->db_id = ret;
						pqinfo->data = 0;
						return SPT_OK;
					}
					break;
				case SPT_RD_UP:
					st_insert_info.fs = pdinfo->fs;	
					st_insert_info.endbit = pdinfo->endbit;
					st_insert_info.dataid = cur_data;
					ret = do_module_insert_up_via_r(pclst,
						&st_insert_info,
						pqinfo->data);
					if (ret == SPT_DO_AGAIN) {
						return  SPT_DO_AGAIN;
					} else if (ret >= 0) {
						pqinfo->db_id = ret;
						pqinfo->data = 0;
						pqinfo->vec_id = pdinfo->cur_vecid;
						return SPT_OK;
					}
					break;

				case SPT_RD_DOWN:
					st_insert_info.endbit = pdinfo->endbit;
					if (pdinfo->fs == pdinfo->endbit
							&& pdinfo->endbit < pclst->endbit)
						st_insert_info.fs = find_fs(
								pnew_data , pdinfo->endbit,
								pclst->endbit - pdinfo->endbit);
					else
						st_insert_info.fs = pdinfo->fs;

					ret = do_module_insert_down_via_r(pclst,
							&st_insert_info, pqinfo->data);
					if (ret == SPT_DO_AGAIN) {
						return SPT_DO_AGAIN;
					}

					if (ret >= 0) {
						pqinfo->db_id = ret;
						pqinfo->data = 0;
						pqinfo->vec_id = st_insert_info.hang_vec;
						return SPT_OK;
					}
				break;
			case SPT_UP_DOWN:
					st_insert_info.fs = pdinfo->fs;	
					st_insert_info.endbit = pdinfo->endbit;

					ret = do_module_insert_up_via_d(pclst,
						&st_insert_info,
						pqinfo->data);
					if (ret == SPT_DO_AGAIN) {
						return  SPT_DO_AGAIN;
					}
					else if (ret >= 0) {
						pqinfo->db_id = ret;
						pqinfo->data = 0;
						pqinfo->vec_id = pdinfo->cur_vecid;
						return SPT_OK;
					}

				break;
			case SPT_LAST_DOWN:
					st_insert_info.fs = pdinfo->fs;	
					st_insert_info.endbit = pdinfo->endbit;
					
					ret = do_module_insert_last_down(pclst,
						&st_insert_info,
						pqinfo->data);
					if (ret == SPT_DO_AGAIN)
						return  SPT_DO_AGAIN;
					else if (ret >= 0) {
						pqinfo->db_id = ret;
						pqinfo->data = 0;
						pqinfo->vec_id = pdinfo->cur_vecid;
						return SPT_OK;
					}
				break;
			default: 
					break;
			}

	default: 
		break;
	}
	return ret;
}

int get_module_data_id(struct module_cluster_head_t *pclst, struct spt_module_vec *pvec)
{
	struct spt_module_vec *pcur, *pnext, *ppre;
	struct spt_module_vec tmp_vec, cur_vec, next_vec;

get_id_start:
	ppre = NULL;
	cur_vec.val = pvec->val;
	pcur = pvec;
	
	if (cur_vec.status == SPT_VEC_INVALID)
		return SPT_DO_AGAIN;

	while (1) {
		if (cur_vec.type == SPT_VEC_RIGHT) {

			pnext = (struct spt_module_vec *)vec_module_id_2_ptr(pclst,
					cur_vec.rd);
			next_vec.val = pnext->val;
			
			if (next_vec.status == SPT_VEC_VALID
				&& next_vec.down != SPT_MODULE_NULL) {
				ppre = pcur;
				pcur = pnext;
				cur_vec.val = next_vec.val;
				continue;
			}
			if (next_vec.status == SPT_VEC_INVALID)
				spt_assert(0);	

			if (next_vec.down == SPT_MODULE_NULL) {
				tmp_vec.val = next_vec.val;
				tmp_vec.status = SPT_VEC_INVALID;
				atomic_cmpxchg((atomic_t *)pnext,
					next_vec.val, tmp_vec.val);
				//set invalid succ or not, refind from cur
				cur_vec.val = pcur->val;
				if (cur_vec.status != SPT_VEC_VALID)
					goto get_id_start;
				continue;
			}
		}  else
			return cur_vec.rd;

		ppre = pcur;
		pcur = pnext;
		cur_vec.val = next_vec.val;
	}
}
int find_module_lowest_data(struct module_cluster_head_t *pclst,
	struct spt_module_vec *start_vec)
{
	struct spt_module_vec *pcur, cur_vec;

	pcur = start_vec;
	cur_vec.val = pcur->val;

	while (1) {
		if (cur_vec.type == SPT_VEC_DATA) {
			if (cur_vec.down == SPT_MODULE_NULL)
				return cur_vec.rd;

			pcur = (struct spt_module_vec *)vec_module_id_2_ptr(pclst,
					cur_vec.down);
			cur_vec.val = pcur->val;
		} else if (cur_vec.type == SPT_VEC_RIGHT) {
			if (cur_vec.down == SPT_MODULE_NULL)
				pcur = (struct spt_vec *)vec_module_id_2_ptr(pclst,
					cur_vec.rd);
			else
				pcur = (struct spt_vec *)vec_module_id_2_ptr(pclst,
					cur_vec.down);
			cur_vec.val = pcur->val;
		} else {
			spt_debug("type error¥r¥n");
			spt_assert(0);
		}
	}
}

int query_data_from_module_tree(struct module_cluster_head_t *pclst, char *pdata, struct module_query_info_t *pqinfo)
{
	struct spt_module_vec *start_vec, *pcur, *pnext, cur_vec, next_vec;
	struct spt_module_vec *vec_stack[16];
	struct spt_module_dh_ext *pdh_ext;
	int    vec_id_stack[16];
	struct vec_cmpret_t cmpres;
	int vec_stack_top = 0;
	u64 startbit,endbit, fs_pos;
	int cur_data, cur_vecid, next_vecid, i, len, first_chbit, cmp;
	char *pcur_data;
	int ret = SPT_NOT_FOUND;

	cur_vecid = 0;
	startbit = 0;
	endbit = pclst->endbit;
	pcur = (struct spt_module_vec *)(void *)pclst->vec_mem;
	cur_vec.val = pcur->val;
	vec_stack_top = 0;
	fs_pos = find_fs(pdata, startbit, endbit - startbit);
	vec_id_stack[vec_stack_top] = cur_vecid;
	vec_stack[vec_stack_top++] = pcur;
	
	while (startbit < endbit) {

		if (fs_pos != startbit) 
			goto prediction_down;

prediction_right:
		if (cur_vec.type == SPT_VEC_DATA) { 
			len = endbit - startbit;
			cur_data = cur_vec.rd;

			if (cur_data >= 0 && cur_data < SPT_DB_MODULE_INVALID) {
				pdh_ext = (struct spt_module_dh_ext *)db_module_id_2_ptr(pclst,
						cur_data);
				pcur_data = pdh_ext->data;
				first_chbit = get_first_change_bit(pdata,
						pcur_data,
						0,
						startbit);
				
				if (first_chbit == -1) {
					cmp = diff_identify(pdata, pcur_data, startbit, len ,&cmpres);
					
					pqinfo->db_id = cur_data;
					pqinfo->vec_id = cur_vecid;
					if (cmp == 0) { 
						pqinfo->cmp_result = 0;
						ret = SPT_OK;
					} else if (cmp > 0)
						pqinfo->cmp_result = 1;
					else
						pqinfo->cmp_result = -1;
					
					return ret;

				} else
					goto check_diff_vec;
			} else 
				spt_assert(0);

		} else {
			pnext = (struct spt_module_vec *)vec_module_id_2_ptr(pclst,
					cur_vec.rd);
			next_vec.val = pnext->val;
			next_vecid = cur_vec.rd;
			
			
			len = next_vec.pos + 1 - startbit;
			startbit += len;
			if (startbit >= endbit)
				spt_assert(0);
			pcur = pnext;
			cur_vecid = next_vecid;
			cur_vec.val = next_vec.val;
			vec_id_stack[vec_stack_top] = cur_vecid;
			vec_stack[vec_stack_top++] = pcur;

			fs_pos = find_fs(pdata,
				startbit,
				endbit - startbit);
		
		}
		continue;
prediction_down:
		
		while (fs_pos > startbit) {

			if (cur_vec.down != SPT_MODULE_NULL)
				goto prediction_down_continue;

			cur_data = get_module_data_id(pclst, pcur);
			
			if (cur_data >= 0 && cur_data < SPT_DB_MODULE_INVALID) {
				pdh_ext = (struct spt_module_dh_ext *)db_module_id_2_ptr(pclst, cur_data);
				pcur_data = pdh_ext->data;
				
				first_chbit = get_first_change_bit(pdata,
						pcur_data,
						0,
						startbit);
				if (first_chbit == -1) {
					pqinfo->db_id = cur_data;
					pqinfo->vec_id = cur_vecid;
					pqinfo->cmp_result = -1;
					return ret;

				} else 
					goto check_diff_vec;
			} else 
				spt_assert(0);

prediction_down_continue:

			pnext = (struct spt_vec *)vec_module_id_2_ptr(pclst,
					cur_vec.down);
			next_vec.val = pnext->val;
			next_vecid = cur_vec.down;

			len = next_vec.pos + 1 - startbit;
			
			if (fs_pos >= startbit + len) {
				startbit += len;
				pcur = pnext;
				cur_vecid = next_vecid;
				cur_vec.val = next_vec.val;
				vec_id_stack[vec_stack_top] = cur_vecid;
				vec_stack[vec_stack_top++] = pcur;
				if (startbit != endbit) {
					continue;
				}
			}

			cur_data = get_module_data_id(pclst, pcur);
			if (cur_data >= 0 && cur_data < SPT_DB_MODULE_INVALID) {
				pdh_ext = (struct spt_module_dh_ext *)db_module_id_2_ptr(pclst, cur_data);
				pcur_data = pdh_ext->data;

			} else 
				spt_assert(0);

			first_chbit = get_first_change_bit(pdata,
						pcur_data,
						0,
						startbit);
			if (first_chbit == -1) {
				pqinfo->db_id = cur_data;
				pqinfo->vec_id = cur_vecid;
				pqinfo->cmp_result = -1;
				return ret;

			} else 
				goto check_diff_vec;
			
		}	
	}

check_diff_vec:
	for (i = 0; i < vec_stack_top; i++) {
		pcur = vec_stack[i];
		if ( i == 0){
			if (first_chbit <= 0)
				spt_assert(0);
			continue;
		}
		if (pcur->pos < first_chbit)
			continue;
		else {
			
			if (vec_module_id_2_ptr(pclst, vec_stack[i-1]->rd) != pcur)
				spt_assert(0);
			cur_data = get_module_data_id(pclst, pcur);
			if (cur_data >= 0 && cur_data < SPT_DB_MODULE_INVALID) {
				pdh_ext = (struct spt_module_dh_ext *)db_module_id_2_ptr(pclst,
						cur_data);
				pcur_data = pdh_ext->data;
				cmp = diff_identify(pdata, pcur_data, startbit, len ,&cmpres);
					
					pqinfo->db_id = cur_data;
					pqinfo->vec_id = vec_id_stack[i - 1];
					if (cmp == 0) { 
						spt_assert(0);
					} else if (cmp > 0)
						pqinfo->cmp_result = 1;
					else
						pqinfo->cmp_result = -1;
				return ret;
			}
			spt_assert(0);

		}
	}
	return ret;
}


int find_data_from_module_cluster(struct module_cluster_head_t *pclst, struct module_query_info_t *pqinfo)
{
	int cur_data, vecid, cmp, op, cur_vecid, pre_vecid, next_vecid;
	struct spt_module_vec *pcur, *pnext, *ppre;
	struct spt_module_vec tmp_vec, cur_vec, next_vec;
	u64 originbit, startbit, endbit, len, fs_pos;
	struct spt_module_dh_ext *pdh_ext;
	u8 direction;
	int ret;
	struct vec_cmpret_t cmpres;
	struct module_data_info_t pinfo;
	char *pdata, *pcur_data;
	u32 check_data_id, check_pos, check_type;

	struct spt_module_vec *pcheck_vec = NULL;
	struct spt_module_vec check_vec;
	char *check_data = NULL;
	int op_type;
	int first_chbit;

	ret = SPT_NOT_FOUND;
	op = pqinfo->op;
	if (op == SPT_OP_INSERT) {
		pdh_ext = (struct spt_module_dh_ext *)pqinfo->data;
		pdata = pdh_ext->data;
	} else
		pdata = pqinfo->data;
	spt_trace("go one find_data process, clst %p¥r¥n", pclst);
	
	cur_data = SPT_DB_MODULE_INVALID;

	refind_start:
	pcur = pqinfo->pstart_vec;
	cur_vecid = pre_vecid = pqinfo->startid;
	/*startpos mainly is zero, find_start vec can input another value, startpos
	 * is equal to the start cur_vec real pos*/
	refind_forward:

	if (pcur == NULL)
		goto refind_start;
	ppre = NULL;
	cur_vecid = pre_vecid;
	pre_vecid = SPT_VEC_MODULE_INVALID;
	cur_vec.val = pcur->val;

	if (pcur == pclst->pstart)
		startbit = pclst->startbit;
	else 
		startbit = cur_vec.pos + 1;
	
	endbit = pqinfo->endbit;
	
	direction = SPT_DIR_START;

	if (cur_data != SPT_DB_MODULE_INVALID)
		cur_data = SPT_DB_MODULE_INVALID;
		
	fs_pos = find_fs(pdata, startbit, endbit-startbit);
	spt_trace("refind_start or forword new_data:%p, startbit:%d, len:%d, fs_pos:%d\r\n",
			pdata, startbit, endbit-startbit, fs_pos);

prediction_start:

	while (startbit < endbit) {
		/*first bit is 1｣ｬcompare with pcur_vec->right*/
		if (fs_pos != startbit) 
			goto prediction_down;
prediction_right:
		
		if (cur_vec.type == SPT_VEC_DATA) { 
			len = endbit - startbit;
			if (cur_data != SPT_DB_MODULE_INVALID) {
				if (cur_vec.rd != cur_data) {
					cur_data = SPT_DB_MODULE_INVALID;
					goto refind_start;
				}
			}
			
			spt_trace("go right data-fs_pos:%d,curbit:%d,datalen:%d\r\n", fs_pos, startbit, len);	
			
			cur_data = cur_vec.rd;
			spt_trace("rd id:%d\r\n",cur_data);	

			if (cur_data >= 0 && cur_data < SPT_DB_MODULE_INVALID) {
				pdh_ext = (struct spt_module_dh_ext *)db_module_id_2_ptr(pclst,
						cur_data);
				pcur_data = pdh_ext->data;
				spt_trace("rd data:%p\r\n",pcur_data);	
				
				first_chbit = get_first_change_bit(pdata,
						pcur_data,
						0,
						startbit);
				
				spt_trace("check endbit:%d, changebit:%d\r\n", startbit, first_chbit);	
				
				if (first_chbit == -1) {
					cmp = diff_identify(pdata, pcur_data, startbit, len ,&cmpres);
					if (cmp == 0) {
						spt_trace("find same record\r\n");	
						goto same_record;
					} else {
						pinfo.cur_vec = cur_vec;
						pinfo.pcur = pcur;
						pinfo.cur_data_id = cur_data;
						pinfo.cur_vecid = cur_vecid;
						pinfo.fs = cmpres.smallfs;
						pinfo.cmp_pos = cmpres.pos;
						pinfo.startbit = startbit;
						pinfo.endbit = startbit + len;
						if (pinfo.cmp_pos <= startbit || pinfo.fs <= pinfo.cmp_pos)
							spt_assert(0);
						if (cmp > 0)
							op_type = SPT_RD_UP;
						else
							op_type = SPT_RD_DOWN;

						if (cmp > 0) {
							spt_trace("final vec process type: RD UP\r\n");
						} else {
							spt_trace("final vec process type: RD DOWN\r\n");
						}
						
						spt_trace("final vec cmp_pos:%d,fs:%d\r\n", cmpres.pos, cmpres.smallfs);
						
						ret = final_module_vec_process(pclst, pqinfo, &pinfo, op_type);
						if (ret == SPT_DO_AGAIN)
							goto refind_start;
						return ret;
					}

				} else { 
					check_pos = first_chbit;
					check_data_id = cur_data;
					check_data = pcur_data;
					
					spt_trace("checkbit:%d,checkdata_id:%d,checkdata:%p\r\n",check_pos ,check_data_id, check_data);	
					goto prediction_check;
				}
			} else if (cur_data == SPT_MODULE_NULL) {
				if (ppre != NULL) {
			/*delete data rd = NULL*/
					spt_trace("continue line %d\r\n", __LINE__);	
					goto refind_start;
				
				}	
				pinfo.cur_vec = cur_vec;
				pinfo.pcur = pcur;
				pinfo.cur_vecid = cur_vecid;
				pinfo.cur_data_id = cur_data;
				pinfo.startbit = startbit;

				spt_trace("first set pcur is %p\r\n",pcur);	
				
				ret = final_module_vec_process(pclst, pqinfo, &pinfo, SPT_FIRST_SET);
				if (ret == SPT_DO_AGAIN)
					goto refind_start;
				return ret;

			} else 
				spt_assert(0);
		} else {
			pnext = (struct spt_module_vec *)vec_module_id_2_ptr(pclst,
					cur_vec.rd);
			next_vec.val = pnext->val;
			next_vecid = cur_vec.rd;
			
			spt_trace("go right vec-fs_pos:%d,startbit:%d\r\n",fs_pos,startbit);	
			spt_trace("next rd:%d,next vec:%p\r\n", next_vecid, pnext);
			
			len = next_vec.pos + 1 - startbit;
			startbit += len;
			if (startbit >= endbit)
				spt_assert(0);
			ppre = pcur;
			pcur = pnext;
			pre_vecid = cur_vecid;
			cur_vecid = next_vecid;
			cur_vec.val = next_vec.val;
			direction = SPT_RIGHT;

			///TODO:startbit already >= DATA_BIT_MAX
			fs_pos = find_fs(pdata,
				startbit,
				endbit - startbit);
			spt_trace("next fs_pos:%d,startbit:%d, len:%d\r\n",fs_pos,startbit, endbit - startbit);	
		
		}
		continue;
prediction_down:
		
		while (fs_pos > startbit) {

			spt_trace("pcur vec:%p, cur_vecid:0x%x ,curpos:%d\r\n",pcur, cur_vecid, startbit);

			if (cur_vec.down != SPT_MODULE_NULL)
				goto prediction_down_continue;
			if (direction == SPT_RIGHT) {
				tmp_vec.val = cur_vec.val;
				tmp_vec.status = SPT_VEC_INVALID;
				cur_vec.val = atomic_cmpxchg(
						(atomic_t *)pcur,
						cur_vec.val, tmp_vec.val);
				/*set invalid succ or not, refind from ppre*/
				pcur = ppre;
				goto refind_forward;
			}

			cur_data = get_module_data_id(pclst, pcur);
			
			spt_trace("cur vec down null , cur_data id %d \r\n",cur_data);

			if (cur_data >= 0 && cur_data < SPT_DB_MODULE_INVALID) {
				pdh_ext = (struct spt_module_dh_ext *)db_module_id_2_ptr(pclst, cur_data);
			
				pcur_data = pdh_ext->data;
				spt_trace("cur vec down null , cur_data %p \r\n",pcur_data);
				
				first_chbit = get_first_change_bit(pdata,
						pcur_data,
						0,
						startbit);

				spt_trace("check endbit:%d,changebit:%d\r\n", startbit, first_chbit);	
				
				if (first_chbit != -1) {	
					check_pos = first_chbit;
					check_data_id = cur_data;
					check_data = pcur_data;
					
					spt_trace("checkbit:%d,checkdata_id:%d,checkdata:%p\r\n",check_pos ,check_data_id, check_data);	
					
					goto prediction_check;
				}
			} else if (cur_data == SPT_MODULE_NULL) {
				if (ppre)
					spt_assert(0);
				printf("debug line is %d\r\n", __LINE__);

			} else {
				if (cur_data == SPT_DO_AGAIN){
					goto refind_start;
				}
				printf("cur_data is %d\r\n", cur_data);
				spt_assert(0);
			}

			/*last down */
			pinfo.cur_vec = cur_vec;
			pinfo.pcur = pcur;
			pinfo.cur_data_id = cur_data;
			pinfo.cur_vecid = cur_vecid;
			pinfo.fs = fs_pos;
			pinfo.startbit = startbit;

			spt_trace("final vec process last down \r\n");

			ret = final_module_vec_process(pclst, pqinfo, &pinfo, SPT_LAST_DOWN);
			if (ret == SPT_DO_AGAIN)
				goto refind_start;

			return ret;

prediction_down_continue:

			pnext = (struct spt_vec *)vec_module_id_2_ptr(pclst,
					cur_vec.down);
			next_vec.val = pnext->val;
			next_vecid = cur_vec.down;

			spt_trace("down continue fs_pos:%d,startbit:%d\r\n",fs_pos,startbit);
			spt_trace("next down vec id:%d,vec:%p\r\n", next_vecid, pnext);
			
			len = next_vec.pos + 1 - startbit;
			
			direction = SPT_DOWN;
			
			spt_trace("next down vec len:%d\r\n",len);

			if (fs_pos >= startbit + len) {
				startbit += len;
				ppre = pcur;
				pcur = pnext;
				pre_vecid = cur_vecid;
				cur_vecid = next_vecid;
				cur_vec.val = next_vec.val;
				if (startbit != endbit) {
					continue;
				}
				//printf("fs is %d, startbit is %d¥r¥n", fs_pos, startbit);
			}

			cur_data = get_module_data_id(pclst, pnext);
			if (cur_data >= 0 && cur_data < SPT_DB_MODULE_INVALID) {
				pdh_ext = (struct spt_module_dh_ext *)db_module_id_2_ptr(pclst, cur_data);
				pcur_data = pdh_ext->data;

			} else { 
				cur_data = SPT_DB_MODULE_INVALID;
				goto refind_start;
			}
			
			spt_trace("down data id :%d down data:%p\r\n", cur_data, pcur_data);	
			
			first_chbit = get_first_change_bit(pdata,
						pcur_data,
						0,
						startbit);
			
			spt_trace("next down vec dismatch\r\n");
			spt_trace("check endbit:%d,changebit:%d\r\n", startbit,first_chbit);	
			
			if (first_chbit != -1) {
				check_pos = first_chbit;
				check_data_id = cur_data;
				check_data = pcur_data;
				
				spt_trace("checkbit:%d,checkdata_id:%d,checkdata:%p\r\n",check_pos ,check_data_id, check_data);	
				goto prediction_check;
			} else {
				if (startbit == endbit) {
					spt_trace("find same record\r\n");	
					goto same_record;
				}
			}

			pinfo.cur_vec = cur_vec;
			pinfo.pcur = pcur;
			pinfo.cur_data_id = cur_data;
			pinfo.cur_vecid = cur_vecid;
			pinfo.fs = fs_pos;
			pinfo.startbit = startbit;
			pinfo.pnext = pnext;

			spt_trace("down up pcur %p\r\n", pcur);
			spt_trace("final vec fs:%d\r\n", fs_pos);
			
			ret = final_module_vec_process(pclst, pqinfo, &pinfo, SPT_UP_DOWN);
			if (ret == SPT_DO_AGAIN)
				goto refind_start;
			
			return ret;
		
		}	
	}

prediction_check:
  	
	spt_trace("prediction check start\r\n");

	cur_data = SPT_DB_MODULE_INVALID;
	pcur = pqinfo->pstart_vec;
	cur_vecid = pre_vecid = pqinfo->startid;
	ppre = NULL;
	cur_vecid = pre_vecid;
	pre_vecid = SPT_VEC_MODULE_INVALID;
	cur_vec.val = pcur->val;
	pcheck_vec = NULL;
	check_type = -1;
	
	if (pcur == pclst->pstart)
		startbit = pclst->startbit;
	else
		startbit = cur_vec.pos + 1;
	
	endbit = pqinfo->endbit;
	
	if (cur_vec.status == SPT_VEC_INVALID) {
		if (pcur == pqinfo->pstart_vec)
			return SPT_DO_AGAIN;
		goto refind_start;
	}
	if (cur_data != SPT_DB_MODULE_INVALID)
		cur_data = SPT_DB_MODULE_INVALID;
	
	direction = SPT_DIR_START;

	fs_pos = find_fs(pdata, startbit, endbit-startbit);
	
	spt_trace("prediction check start new_data:%p, startbit:%d, len:%d, fs_pos:%d\r\n",
			pdata, startbit, endbit-startbit, fs_pos);
	
	while (startbit < endbit) {
		/*first bit is 1｣ｬcompare with pcur_vec->right*/
		if (fs_pos != startbit)
			goto go_down;
		spt_trace("rd pcur vec:%p, cur_vecid:0x%x, curpos:%d\r\n",
				pcur, cur_vecid, startbit);
go_right:
		if (cur_vec.type == SPT_VEC_DATA) { 
			
			len = endbit - startbit;
			cur_data = cur_vec.rd;

			if (cur_data >= 0 && cur_data < SPT_DB_MODULE_INVALID) {
				if (cur_data != check_data_id)
					spt_assert(0);
				if (!pcheck_vec)
					spt_assert(0);
			} else if (cur_data == SPT_MODULE_NULL) {
					goto refind_start;
			} else
				spt_assert(0);
			
			spt_trace("prediction check ok, rd end \r\n");
			
			ret = final_module_vec_process(pclst, pqinfo, &pinfo, check_type);
			if (ret == SPT_DO_AGAIN)
				goto refind_start;
			return ret;

		} else {
			pnext = (struct spt_module_vec *)vec_module_id_2_ptr(pclst,
					cur_vec.rd);
			next_vec.val = pnext->val;
			next_vecid = cur_vec.rd;
			
			len = next_vec.pos + 1 - startbit;
	
			if (startbit + len > check_pos) {
				pcheck_vec = pcur;
				check_vec = cur_vec;
				if (cur_data == SPT_DB_MODULE_INVALID &&
						cur_vec.type != SPT_VEC_DATA) {
					cur_data = get_module_data_id(pclst, pnext);
					if(cur_data == SPT_DO_AGAIN) {
						cur_data = SPT_DB_MODULE_INVALID;
						goto refind_start;
					}
					if (cur_data == SPT_MODULE_NULL)
						spt_assert(0);
				}
				
				cmp = diff_identify(pdata, check_data, startbit, len, &cmpres);
				pinfo.cur_vec = cur_vec;
				pinfo.pcur = pcur;
				pinfo.cur_data_id = cur_data;
				pinfo.cur_vecid = cur_vecid;
				pinfo.fs = cmpres.smallfs;
				pinfo.cmp_pos = cmpres.pos;
				pinfo.startbit = startbit;
				pinfo.endbit = startbit + len;
				if (pinfo.cmp_pos <= startbit || pinfo.fs <= pinfo.cmp_pos)
					spt_assert(0);
				
				spt_trace("prediction check ok, check pos:%d, check_vec:%p\r\n", check_pos, pcheck_vec);

				if (cmp == 0) {
					spt_assert(0);
				} else if (cmp > 0) {
					check_type = SPT_RD_UP;
					
					spt_trace("prediction check ok, check type RD UP\r\n");
				} else {
					check_type = SPT_RD_DOWN;
					spt_trace("prediction check ok, check type RD_DOWN\r\n");
				}
				check_pos = endbit + 1;
				ret = final_module_vec_process(pclst, pqinfo, &pinfo, check_type);
				if (ret == SPT_DO_AGAIN)
					goto refind_start;
				return ret;
			}
			startbit += len;
			if (startbit >= endbit)
				spt_assert(0);
			ppre = pcur;
			pcur = pnext;
			pre_vecid = cur_vecid;
			cur_vecid = next_vecid;
			cur_vec.val = next_vec.val;
			direction = SPT_RIGHT;
			///TODO:startbit already >= DATA_BIT_MAX
			fs_pos = find_fs(pdata,
				startbit,
				endbit - startbit);
		}
		
		continue;
		/*first bit is 0｣ｬstart from pcur_vec->down*/
go_down:

		while (fs_pos > startbit) {
			spt_trace("down pcur vec:%p, cur_vecid:0x%x, curpos:%d\r\n",
					pcur, cur_vecid, startbit);
			
			if (cur_vec.down != SPT_MODULE_NULL)
				goto down_continue;
			if (direction == SPT_RIGHT) {
				spt_assert(0);
			}
			
			cur_data = get_module_data_id(pclst, pcur);
			if (cur_data >= 0 && cur_data < SPT_DB_MODULE_INVALID) {
				if (cur_data != check_data_id)
					spt_assert(0);
				if (!pcheck_vec)
					spt_assert(0);
				
				spt_trace("prediction check ok, down end \r\n");
				if (check_type == -1)
					spt_assert(0);

				ret = final_module_vec_process(pclst, pqinfo, &pinfo, check_type);
				if (ret == SPT_DO_AGAIN)
					goto refind_start;
				return ret;

			} else if (cur_data == SPT_MODULE_NULL) {
				goto refind_start;
			} else {
				if (cur_data == SPT_DO_AGAIN) {
					cur_data = SPT_DB_MODULE_INVALID;
					goto refind_start;
				}
				spt_assert(0);
			}
down_continue:
			pnext = (struct spt_module_vec *)vec_module_id_2_ptr(pclst,
					cur_vec.down);
			next_vec.val = pnext->val;
			next_vecid = cur_vec.down;
			
			if (next_vec.status == SPT_VEC_INVALID) {
				spt_assert(0);
			}

			len = next_vec.pos + 1 - startbit;

			direction = SPT_DOWN;
			/* signpost not used now*/
			
			if (fs_pos >= startbit + len) {
				startbit += len;
				ppre = pcur;
				pcur = pnext;
				pre_vecid = cur_vecid;
				cur_vecid = next_vecid;
				cur_vec.val = next_vec.val;
			
				if (startbit != endbit) {
					continue;
				}
			}
			cur_data = get_module_data_id(pclst, pnext);
			if (cur_data >= 0 && cur_data < SPT_DB_MODULE_INVALID) {
				
				if (cur_data != check_data_id)
					spt_assert(0);
				if (!pcheck_vec)
					spt_assert(0);
				spt_trace("prediction check ok, down continue \r\n");
				if (check_type == -1)
					spt_assert(0);

				ret = final_module_vec_process(pclst, pqinfo, &pinfo, check_type);
				if (ret == SPT_DO_AGAIN)
					goto refind_start;
				return ret;

			} else { 
				cur_data = SPT_DB_MODULE_INVALID;
				goto refind_start;
			}
		}
		spt_assert(fs_pos == startbit);
	}

same_record:

	ret = SPT_OK;

	if (cur_data == SPT_DB_MODULE_INVALID) {
		if (cur_vec.type != SPT_VEC_DATA)
			spt_assert(0);
		cur_data = cur_vec.rd;
		pdh_ext = (struct spt_module_dh_ext *)db_module_id_2_ptr(pclst, cur_data);
		pcur_data = pdh_ext->data;
		smp_mb();/* ^^^ */
	}

	switch (op) {
	case SPT_OP_FIND:
		if (pqinfo->op == SPT_OP_FIND) {
			pqinfo->db_id = cur_data;
			pqinfo->vec_id = cur_vecid;
			pqinfo->cmp_result = 0;
		}

		return ret;
	case SPT_OP_INSERT:
		pqinfo->db_id = cur_data;
		pqinfo->vec_id = cur_vecid;
		return ret;

    default: spt_assert(0);
        break;
    }
    
    return SPT_ERR;
}

struct module_cluster_head_t * spt_module_tree_init(u64 endbit, int data_total)
{
	struct module_cluster_head_t *pclst;
	struct spt_module_dh_ext *pdh_ext,*pcur_dh_ext, *pnext_dh_ext;
	struct module_query_info_t qinfo = {0};
	struct spt_module_vec *pvec_start;
	char *pdata;
	int i, j;
	
	pclst = spt_module_cluster = module_cluster_init(HASH_WINDOW_LEN*8, data_total);

	if (pclst == NULL)
		return NULL;
	
	for (i = 0; i < data_total; i++) {
		pdh_ext = spt_malloc(sizeof (struct spt_module_dh_ext) + HASH_WINDOW_LEN);
		if (!pdh_ext) {
			spt_assert(0);
		}
		pdh_ext->data = (char *)(pdh_ext + 1);
		pdata = pdh_ext->data;
		module_array_data[i] = (char *)(void *)pdh_ext;

		if (i != data_total -1)
			get_random_string(pdata, HASH_WINDOW_LEN);
		else 
			memset(pdata, 0xFF, HASH_WINDOW_LEN);
	}

	for (i = 0; i < data_total; i++) {
		for (j = 0; j < data_total - i -1; j++) {
			pcur_dh_ext = module_array_data[j];
			pnext_dh_ext = module_array_data[j + 1];
			if (memcmp(pcur_dh_ext->data, pnext_dh_ext->data, HASH_WINDOW_LEN) > 0) {
				module_array_data[j] = pnext_dh_ext;
				module_array_data[j + 1] = pcur_dh_ext;
			}
		}
	}
	j = 0;
	for (i = data_total - 1; i >= 0; i--) {
		pcur_dh_ext = module_array_data[i];

		pnext_dh_ext = pclst->db_mem + j *(sizeof(struct spt_module_dh_ext) + HASH_WINDOW_LEN);
		pnext_dh_ext->dataid = j;
		pnext_dh_ext->data = (char *)(pnext_dh_ext + 1);
		j++;
		memcpy(pnext_dh_ext->data, pcur_dh_ext->data, HASH_WINDOW_LEN);	
		
		pvec_start = (struct spt_module_vec *)vec_module_id_2_ptr(pclst, pclst->vec_head);

		qinfo.op = SPT_OP_INSERT;
		qinfo.pstart_vec = pvec_start;
		qinfo.startid = pclst->vec_head;
		qinfo.endbit = pclst->endbit;
		qinfo.data = pnext_dh_ext;

		find_data_from_module_cluster(pclst, &qinfo);
	}

	return spt_module_cluster;	
}
#if 0
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
#endif
unsigned int module_tree_alloc_ok;
unsigned int module_tree_alloc_conf;
int module_tree_hash_one_byte;
int module_tree_hash_two_byte;

int get_vec_by_module_tree(struct cluster_head_t *vec_clst, char *pdata, int pos,
		struct spt_vec *cur_vec,
		struct spt_vec **ret_vec,
		unsigned int *window_hash, unsigned int *seg_hash)
{
	struct module_query_info_t qinfo;
	int ret;
	struct spt_module_dh_ext *pdext_h;
	struct spt_module_vec *phang_vec, *pvec, vec;
	char *cur_byte,*window_byte, *module_data;
	int window_num, window_len, cur_pos_len;
	unsigned int grama_seg_hash;
	int vecid = -1;
	int j;
	int cnt = 0;
	struct module_cluster_head_t * pclst = spt_module_cluster;

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
		memset(&qinfo, 0, sizeof(struct module_query_info_t));
		qinfo.op = SPT_OP_FIND;
		qinfo.pstart_vec = pclst->pstart;
		qinfo.startid = pclst->vec_head;
		qinfo.endbit = pclst->endbit;
		qinfo.data = window_byte;
		cnt++;
		ret = find_data_from_module_cluster(pclst, &qinfo);
		if (ret >= 0) {
			pdext_h = (struct spt_module_dh_ext *)db_module_id_2_ptr(pclst, qinfo.db_id);
			
			if (qinfo.cmp_result < 0) {
				/* < 0 continue to look down */
				pvec = (struct spt_module_vec *)vec_module_id_2_ptr(pclst,
						qinfo.vec_id);
				vec.val = pvec->val;
				/* difference appear in leaf node*/
				if (vec.type != SPT_VEC_DATA) {
					pvec = (struct spt_module_vec *)vec_module_id_2_ptr(pclst, vec.rd);
					ret = find_module_lowest_data(pclst, pvec);
					if (ret == SPT_MODULE_NULL)
						spt_assert(0);
					pdext_h = (struct spt_module_dh_ext *)db_module_id_2_ptr(pclst, ret);
				}

			} else if (qinfo.cmp_result > 0){
				/* > 0 continue to look from hang vec*/
				if (pdext_h->hang_vec >= SPT_INVALID)
					spt_assert(0);
				phang_vec = (struct spt_module_vec *)vec_module_id_2_ptr(pclst,
						pdext_h->hang_vec);
				vec.val = phang_vec->val;
				if (vec.type == SPT_VEC_DATA) {
					if (vec.rd == SPT_MODULE_NULL)
						spt_assert(0);
					pdext_h = (struct spt_module_dh_ext *)db_module_id_2_ptr(pclst,
							vec.rd);

				} else {
					pvec = (struct spt_module_vec *)vec_module_id_2_ptr(pclst, vec.rd);
					ret = find_module_lowest_data(pclst, pvec);
					if (ret == SPT_MODULE_NULL)
						spt_assert(0);
					pdext_h = (struct spt_module_dh_ext *)db_module_id_2_ptr(pclst, ret);
				}

			}
		} else {
			spt_debug("find_data err!¥r¥n");
			spt_assert(0);
		}
		spt_trace("pdext_h is %p \r\n", pdext_h);
		module_data = pdext_h->data;
		spt_trace("module data  %p\r\n", module_data);
#if 0
		for (j = 0; j < HASH_WINDOW_LEN; j++)
			spt_trace("%2x ", module_data[j]);

		spt_trace("window_byte is %p\r\n",window_byte);
		for (j = 0; j < HASH_WINDOW_LEN; j++)
			spt_trace("%2x ", window_byte[j]);
		spt_trace("\r\n");
#endif
		*window_hash = grama_seg_hash;
		*seg_hash = djb_hash_seg(module_data, grama_seg_hash, 8);	
		vecid = vec_judge_full_and_alloc(vec_clst, ret_vec, *seg_hash);
		if (vecid != -1) {
			if ((cnt == 1) && (pos > 192 *8))
				module_tree_hash_one_byte++;
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
	struct module_query_info_t qinfo;
	struct spt_module_dh_ext *pdext_h;
	struct spt_module_vec *phang_vec, *pmvec, vec;
	struct spt_vec *pvec;
	char *cur_byte, *window_byte, *module_data;
	int window_num, window_len, cur_pos_len;
	unsigned int grama_seg_hash, window_hash, seg_hash;
	int vecid = -1;
	int i = 0, j, gid;
	int ret;
	struct spt_grp *grp;
	struct module_cluster_head_t * pclst = spt_module_cluster;

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
		memset(&qinfo, 0, sizeof(struct module_query_info_t));
		qinfo.op = SPT_OP_FIND;
		qinfo.pstart_vec = pclst->pstart;
		qinfo.startid = pclst->vec_head;
		qinfo.endbit = pclst->endbit;
		qinfo.data = window_byte;
		
		PERF_STAT_START(find_stable_tree);
		ret = query_data_from_module_tree(pclst,window_byte, &qinfo);
		PERF_STAT_END(find_stable_tree);
		if (ret >= 0) {
			pdext_h = (struct spt_module_dh_ext *)db_module_id_2_ptr(pclst, qinfo.db_id);
			
			if (qinfo.cmp_result < 0) {
				/* < 0 continue to look down */
				pmvec = (struct spt_module_vec *)vec_module_id_2_ptr(pclst,
						qinfo.vec_id);
				vec.val = pmvec->val;
				/* difference appear in leaf node*/
				if (vec.type != SPT_VEC_DATA) {
					pmvec = (struct spt_module_vec *)vec_module_id_2_ptr(pclst, vec.rd);
					ret = find_module_lowest_data(pclst, pmvec);
					if (ret == SPT_MODULE_NULL)
						spt_assert(0);
					pdext_h = (struct spt_module_dh_ext *)db_module_id_2_ptr(pclst, ret);
				}

			} else if (qinfo.cmp_result > 0){
				/* > 0 continue to look from hang vec*/
				if (pdext_h->hang_vec >= SPT_VEC_MODULE_INVALID)
					spt_assert(0);
				phang_vec = (struct spt_module_vec *)vec_module_id_2_ptr(pclst,
						pdext_h->hang_vec);
				vec.val = phang_vec->val;
				if (vec.type == SPT_VEC_DATA) {
					if (vec.rd == SPT_MODULE_NULL)
						spt_assert(0);
					pdext_h = (struct spt_module_dh_ext *)db_module_id_2_ptr(pclst,
							vec.rd);

				} else {
					pmvec = (struct spt_module_vec *)vec_module_id_2_ptr(pclst, vec.rd);
					ret = find_module_lowest_data(pclst, pmvec);
					if (ret == SPT_MODULE_NULL)
						spt_assert(0);
					pdext_h = (struct spt_module_dh_ext *)db_module_id_2_ptr(pclst, ret);
				}

			}
		} else {
			spt_debug("find_data err!¥r¥n");
			spt_assert(0);
		}
		spt_trace("pdext_h is %p \r\n", pdext_h);
		module_data = pdext_h->data;
		spt_trace("module data  %p\r\n", module_data);
#if 0
		for (j = 0; j < HASH_WINDOW_LEN; j++)
			spt_trace("%2x ", module_data[j]);

		spt_trace("window_byte is %p\r\n",window_byte);
		for (j = 0; j < HASH_WINDOW_LEN; j++)
			spt_trace("%2x ", window_byte[j]);
		spt_trace("\r\n");
#endif

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
