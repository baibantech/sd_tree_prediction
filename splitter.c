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
 * ONE(Object Non-duplicate Engine) sd-tree algorithm
 *
 * Contact Information:
 * info-linux <info@baibantech.com.cn>
 */

#include "chunk.h"
#include "spt_dep.h"
#include "spt_thread.h"
#include "splitter.h"

struct cluster_head_t *pgclst;


void spt_set_errno(int val)
{
	g_thrd_errno = val;
}
int spt_get_errno(void)
{
	return g_thrd_errno;
}
int spt_get_thrd_id(void)
{
	return g_thrd_id;
}

void spt_bit_clear(u8 *p, u64 start, u64 len)
{
	u8 bitstart, bitend;
	s64 lenbyte;
	u8 *acstart;

	bitstart = start%8;
	bitend = (bitstart + len)%8;
	acstart = p + start/8;
	lenbyte =  (bitstart + len)/8;

	if (bitstart != 0) {
		*acstart = *acstart >> (8-bitstart) << (8-bitstart);
		acstart++;
	}
	while (lenbyte > 0) {
		*acstart = 0;
		acstart++;
		lenbyte--;
	}
	if (bitend != 0) {
		*acstart = *acstart << bitend;
		*acstart = *acstart >> bitend;
	}
}

void spt_bit_cpy(u8 *to, const u8 *from, u64 start, u64 len)
{
	u8 bitstart, bitend;
	s64 lenbyte;
	u8 uca, ucb;
	u8 *acstart;
	u8 *bcstart;

	bitstart = start%8;
	bitend = (bitstart + len)%8;
	acstart = to + start/8;
	bcstart = (u8 *)from + start/8;
	lenbyte =  (bitstart + len)/8;

	if (bitstart != 0) {
		uca = *acstart >> (8-bitstart) << (8-bitstart);
		ucb = *bcstart << bitstart;
		ucb = ucb >> bitstart;
		*acstart = uca | ucb;
		acstart++;
		bcstart++;
		lenbyte--;
	}

	while (lenbyte > 0) {
		*acstart = *bcstart;
		acstart++;
		bcstart++;
		lenbyte--;
	}

	if (bitend != 0) {
		uca = *acstart << bitend;
		uca = uca >> bitend;
		ucb = *bcstart >> (8-bitend) << (8-bitend);
		*acstart = uca | ucb;
		acstart++;
		bcstart++;
	}
}

void spt_stack_init(struct spt_stack *p_stack, int size)
{
	p_stack->p_bottom = (void **)spt_malloc(size * sizeof(void *));

	if (p_stack->p_bottom == NULL) {
		spt_print("\r\n%d\t%s", __LINE__, __func__);
		return;
	}
	p_stack->p_top = p_stack->p_bottom;
	p_stack->stack_size = size;
}

int spt_stack_full(struct spt_stack *p_stack)
{
	return p_stack->p_top - p_stack->p_bottom >= p_stack->stack_size-1;
}

int spt_stack_empty(struct spt_stack *p_stack)
{
	return p_stack->p_top <= p_stack->p_bottom;
}

void spt_stack_push(struct spt_stack *p_stack, void *value)
{
	struct spt_stack *p_tmp = (struct spt_stack *)p_stack->p_bottom;

	if (spt_stack_full(p_stack)) {
		p_stack->p_bottom = (void **)spt_realloc(p_stack->p_bottom,
		2*p_stack->stack_size*sizeof(void *));

		if (!p_stack->p_bottom) {
			spt_free(p_tmp);
			spt_print("\r\n%d\t%s", __LINE__, __func__);
			return;
		}
		p_stack->stack_size = 2*p_stack->stack_size;
	}
	(p_stack->p_top)++;
	*(p_stack->p_top) = value;
}

void *spt_stack_pop(struct spt_stack *p_stack)
{
	void *value = 0;

	if (spt_stack_empty(p_stack))
		return (void *)-1;

	value = *(p_stack->p_top--);

	return value;
}

void spt_stack_destroy(struct spt_stack *p_stack)
{
	spt_free(p_stack->p_bottom);
//	spt_free(p_stack);
	p_stack->stack_size = 0;
	p_stack->p_bottom = NULL;
	p_stack->p_top = NULL;
	p_stack = NULL;
}
u64 ullfind_firt_set(u64 dword)
{
	int i;

	for (i = 63; i >= 0; i--) {
		if (dword >> i != 0)
			return 63-i;
	}

	return 64;
}
u64 uifind_firt_set(u32 word)
{
	int i;

	for (i = 31; i >= 0; i--) {
		if (word >> i != 0)
			return 31-i;
	}

	return 32;
}
u64 usfind_firt_set(u16 word)
{
	int i;

	for (i = 15; i >= 0; i--) {
		if (word >> i != 0)
			return 15-i;
	}

	return 16;
}
u64 ucfind_firt_set(u8 byte)
{
	int i;

	for (i = 7; i >= 0; i--) {
		if (byte >> i != 0)
			return 7-i;
	}
	return 8;
}

char *spt_upper_construct_data(char *pkey)
{
	struct spt_dh_ext *pext_head;
	char *pdata;

	pext_head = (struct spt_dh_ext *)spt_malloc(
			sizeof(struct spt_dh_ext)+DATA_SIZE);
	if (pext_head == NULL)
		return NULL;

	pdata = (char *)(pext_head + 1);
	pext_head->data = pdata;
	pext_head->plower_clst = NULL;
	memcpy(pdata, pkey, DATA_SIZE);
	return (char *)pext_head;
}

char *get_real_data(struct cluster_head_t *pclst, char *pdata)
{
	struct spt_dh_ext *ext_head;

	if (pclst->is_bottom)
		return pdata;
	ext_head = (struct spt_dh_ext *)pdata;
	return ext_head->data;
}
/**
 * get_data_id - get the data id of a vector
 * @pclst: pointer of sd tree cluster head
 * @pvec: pointer of a vector in the cluster
 *
 * the data id is record in the leaf node
 * a vector's data id equal it's right vector
 */
int get_data_id(struct cluster_head_t *pclst, struct spt_vec *pvec)
{
	struct spt_vec *pcur, *pnext, *ppre;
	struct spt_vec tmp_vec, cur_vec, next_vec;
	//u8 direction;
	u32 vecid;
	int ret;

get_id_start:
	ppre = 0;
	cur_vec.val = pvec->val;
	pcur = pvec;
	if (cur_vec.status == SPT_VEC_RAW) {
		smp_mb();/* ^^^ */
		cur_vec.val = pvec->val;
		if (cur_vec.status == SPT_VEC_RAW)
			return SPT_DO_AGAIN;
	}
	if (cur_vec.status == SPT_VEC_INVALID)
		return SPT_DO_AGAIN;

	while (1) {
		if (cur_vec.type == SPT_VEC_RIGHT) {
			pnext = (struct spt_vec *)vec_id_2_ptr(pclst,
					cur_vec.rd);
			next_vec.val = pnext->val;
			if (next_vec.status == SPT_VEC_VALID
				&& next_vec.down != SPT_NULL) {
				ppre = pcur;
				pcur = pnext;
				cur_vec.val = next_vec.val;
				continue;
			}
			if (next_vec.status == SPT_VEC_RAW) {
				smp_mb();/* ^^^ */
				next_vec.val = pnext->val;
				if (next_vec.status == SPT_VEC_RAW)
					goto get_id_start;
			}
			if (next_vec.status == SPT_VEC_INVALID) {
				tmp_vec.val = cur_vec.val;
				vecid = cur_vec.rd;
				tmp_vec.rd = next_vec.rd;
				if (next_vec.type == SPT_VEC_DATA
					&& next_vec.down == SPT_NULL) {
					spt_set_data_flag(tmp_vec);
					if (next_vec.rd == SPT_NULL
						&& pcur != pclst->pstart)
						tmp_vec.status
						= SPT_VEC_INVALID;
				}
				if (next_vec.type == SPT_VEC_DATA
					&& next_vec.down != SPT_NULL)
					tmp_vec.rd = next_vec.down;

				if (cur_vec.val == atomic64_cmpxchg(
					(atomic64_t *)pcur,
					cur_vec.val,
					tmp_vec.val)) {
                    vec_free(pclst, vecid);
					ret = SPT_OK;
					if (ret != SPT_OK)
						return ret;
				}
				cur_vec.val = pcur->val;
				if (cur_vec.status != SPT_VEC_VALID)
					goto get_id_start;
				continue;
			}

			if (next_vec.down == SPT_NULL) {
				tmp_vec.val = next_vec.val;
				tmp_vec.status = SPT_VEC_INVALID;
				atomic64_cmpxchg((atomic64_t *)pnext,
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

/* insert the first data that its first bit is 1*/
int do_insert_first_set(struct cluster_head_t *pclst,
	struct insert_info_t *pinsert,
	char *new_data)
{
	u32 dataid;
	struct spt_vec tmp_vec, *pcur;
	struct spt_dh *pdh;
	struct spt_dh_ext *pdh_ext;
	int ret;
	int id;

	if(pinsert->alloc_type == SPT_TOP_INSERT)
		id = pclst->last_alloc_id;
	else
		id = pinsert->key_id;
    dataid = db_alloc_from_grp(pclst, id, &pdh);
	if (!pdh) {
		spt_print("\r\n%d\t%s", __LINE__, __func__);
		return SPT_NOMEM;
	}
	pdh->ref = pinsert->ref_cnt;
	pdh->pdata = new_data;

	pcur = (struct spt_vec *)vec_id_2_ptr(pclst, pclst->vec_head);
//	cur_vec.val = pcur->val;
	spt_assert(pcur == pinsert->pkey_vec);
	tmp_vec.val = pinsert->key_val;
//	tmp_vec.val = cur_vec.val;
	tmp_vec.rd = dataid;
	if (!pclst->is_bottom) {
		pdh_ext = (struct spt_dh_ext *)pdh->pdata;
		pdh_ext->hang_vec = SPT_NULL;
	}
	smp_mb();/* ^^^ */
	if (pinsert->key_val == atomic64_cmpxchg(
		(atomic64_t *)pinsert->pkey_vec,
		pinsert->key_val, tmp_vec.val))
		return dataid;

	spt_set_data_not_free(pdh);
    db_free(pclst, dataid);
    //ret = fill_in_rsv_list(pclst, 1, g_thrd_id);
    //if(ret == SPT_OK)
        // return SPT_DO_AGAIN;
	return ret;
}
/**
 * compare with the data corresponding to a right vector,
 * the new data is greater, so insert it above the vector
 */
int do_insert_up_via_r(struct cluster_head_t *pclst,
	struct insert_info_t *pinsert,
	char *new_data)
{
	struct spt_vec tmp_vec, *pvec_a, *pvec_b, *pvec_s, *pvec_s2;
	u64 signpost;
	u32 dataid, vecid_a, vecid_b, vecid_s, vecid_s2, tmp_rd, cnt;
	struct spt_dh *pdh, *plast_dh;
	struct spt_dh_ext *pdh_ext, *plast_dh_ext;
	int ret;
	int id;

	if(pinsert->alloc_type == SPT_TOP_INSERT)
		id = pclst->last_alloc_id;
	else
		id = pinsert->key_id;
	pvec_b = 0;
	pvec_s = 0;
	pvec_s2 = 0;
    dataid = db_alloc_from_grp(pclst, id, &pdh);
	if (!pdh) {
		spt_print("\r\n%d\t%s", __LINE__, __func__);
		return SPT_NOMEM;
	}
	pdh->ref = pinsert->ref_cnt;
	pdh->pdata = new_data;

	tmp_vec.val = pinsert->key_val;
	signpost = pinsert->signpost;
	tmp_rd = tmp_vec.rd;

    vecid_a = vec_alloc_from_grp(pclst, id, &pvec_a);
	if (pvec_a == 0) {
		spt_print("\r\n%d\t%s", __LINE__, __func__);
		spt_set_data_not_free(pdh);
        db_free(pclst, dataid);
		return SPT_NOMEM;
	}
	pvec_a->val = 0;
	pvec_a->type = SPT_VEC_DATA;
	pvec_a->pos = (pinsert->cmp_pos-1)&SPT_VEC_SIGNPOST_MASK;
	pvec_a->rd = dataid;

	if ((pinsert->cmp_pos-1)-signpost
		> SPT_VEC_SIGNPOST_MASK) {
		vecid_s = vec_alloc_from_grp(pclst, 0, &pvec_s);
		if (pvec_s == 0) {
			spt_print("\r\n%d\t%s", __LINE__, __func__);
			spt_set_data_not_free(pdh);
            db_free(pclst, dataid);
            vec_free(pclst, vecid_a);
			return SPT_NOMEM;
		}
		pvec_s->val = 0;
		pvec_s->idx = (pinsert->cmp_pos-1)>>SPT_VEC_SIGNPOST_BIT;
		pvec_s->rd = vecid_a;

		//pvec_a->down = vecid_b;

		tmp_vec.rd = vecid_s;
		signpost = pvec_s->idx << SPT_VEC_SIGNPOST_BIT;
	} else {
		tmp_vec.rd = vecid_a;
		if (tmp_vec.pos == pvec_a->pos && tmp_vec.pos != 0)
			spt_assert(0);
	}

	if (tmp_vec.type == SPT_VEC_DATA
		|| pinsert->endbit > pinsert->fs) {
        vecid_b = vec_alloc_from_grp(pclst, id, &pvec_b);
		if (pvec_b == 0) {
			spt_print("\r\n%d\t%s", __LINE__, __func__);
			spt_set_data_not_free(pdh);
            db_free(pclst, dataid);
            vec_free(pclst, vecid_a);
			if (pvec_s != 0)
                vec_free(pclst, vecid_s);
			return SPT_NOMEM;
		}
		pvec_b->val = 0;
		pvec_b->type = tmp_vec.type;
		pvec_b->pos = (pinsert->fs-1)&SPT_VEC_SIGNPOST_MASK;
		pvec_b->rd = tmp_rd;
		pvec_b->down = SPT_NULL;
		tmp_vec.type = SPT_VEC_RIGHT;
		if ((pinsert->fs-1)-signpost > SPT_VEC_SIGNPOST_MASK) {
			vecid_s2 = vec_alloc_from_grp(pclst, 0, &pvec_s2);
			if (pvec_s2 == 0) {
				spt_set_data_not_free(pdh);
                db_free(pclst, dataid);
                vec_free(pclst, vecid_a);
                vec_free(pclst, vecid_b);
				if (pvec_s != 0)
                    vec_free(pclst, vecid_s);

				spt_print("\r\n%d\t%s", __LINE__, __func__);
				return SPT_NOMEM;
			}
			pvec_s2->val = 0;
			pvec_s2->idx = (pinsert->fs-1)>>SPT_VEC_SIGNPOST_BIT;
			pvec_s2->rd = vecid_b;

			pvec_a->down = vecid_s2;
		} else
			pvec_a->down = vecid_b;
	} else
		pvec_a->down = tmp_rd;
	if (!pclst->is_bottom) {
		pdh_ext = (struct spt_dh_ext *)pdh->pdata;
		plast_dh = (struct spt_dh *)db_id_2_ptr(pclst, pinsert->dataid);
		plast_dh_ext = (struct spt_dh_ext *)plast_dh->pdata;
		pdh_ext->hang_vec = plast_dh_ext->hang_vec;
	}
	smp_mb();/* ^^^ */
	if (pinsert->key_val == atomic64_cmpxchg(
		(atomic64_t *)pinsert->pkey_vec,
		pinsert->key_val, tmp_vec.val)) {
		if (!pclst->is_bottom)
			plast_dh_ext->hang_vec = vecid_a;
		return dataid;
	}
	spt_set_data_not_free(pdh);
	db_free(pclst, dataid);
	vec_free(pclst, vecid_a);
	cnt = 2;
	if (pvec_b != 0) {
    	vec_free(pclst, vecid_b);
		cnt++;
	}
	if (pvec_s != 0) {
		vec_free(pclst, vecid_s);
		cnt++;
	}
	if (pvec_s2 != 0) {
		vec_free(pclst, vecid_s2);
		cnt++;
	}
	#if 0
	ret = fill_in_rsv_list(pclst, cnt, g_thrd_id);
	if (ret == SPT_OK)
		return SPT_DO_AGAIN;
	#endif
	return ret;
}
/**
 * compare with the data corresponding to a right vector,
 * the new data is smaller, so insert it below the vector
 */
int do_insert_down_via_r(struct cluster_head_t *pclst,
	struct insert_info_t *pinsert,
	char *new_data)
{
	struct spt_vec tmp_vec, *pvec_a, *pvec_b, *pvec_s;
	u64 signpost;
	u32 dataid, vecid_a, vecid_b, vecid_s, cnt;
	struct spt_dh *pdh;
	struct spt_dh_ext *pdh_ext;
	int ret;
	int id;

	if(pinsert->alloc_type == SPT_TOP_INSERT)
		id = pclst->last_alloc_id;
	else
		id = pinsert->key_id;
	pvec_s = 0;
    dataid = db_alloc_from_grp(pclst, id, &pdh);
	if (!pdh) {
		spt_print("\r\n%d\t%s", __LINE__, __func__);
		return SPT_NOMEM;
	}
	pdh->ref = pinsert->ref_cnt;
	pdh->pdata = new_data;

	tmp_vec.val = pinsert->key_val;
	signpost = pinsert->signpost;

    vecid_b = vec_alloc_from_grp(pclst, id, &pvec_b);
	if (!pvec_b) {
		spt_print("\r\n%d\t%s", __LINE__, __func__);
		spt_set_data_not_free(pdh);
        db_free(pclst, dataid);
		return SPT_NOMEM;
	}
	pvec_b->val = 0;
	pvec_b->type = SPT_VEC_DATA;
	pvec_b->pos = (pinsert->fs-1)&SPT_VEC_SIGNPOST_MASK;
	pvec_b->rd = dataid;
	pvec_b->down = SPT_NULL;

    vecid_a = vec_alloc_from_grp(pclst, id, &pvec_a);
	if (!pvec_a) {
		spt_print("\r\n%d\t%s", __LINE__, __func__);
		spt_set_data_not_free(pdh);
        db_free(pclst, dataid);
        vec_free(pclst, vecid_b);
		return SPT_NOMEM;
	}
	pvec_a->val = 0;
//	pvec_a->type = SPT_VEC_DATA;
	pvec_a->pos = (pinsert->cmp_pos-1)&SPT_VEC_SIGNPOST_MASK;
//	pvec_a->rd = dataid;

	if (tmp_vec.type == SPT_VEC_DATA) {
		pvec_a->type = SPT_VEC_DATA;
		pvec_a->rd = tmp_vec.rd;
		tmp_vec.type = SPT_VEC_RIGHT;
	} else {
		pvec_a->type = SPT_VEC_RIGHT;
		pvec_a->rd = tmp_vec.rd;
	}


	if ((pinsert->cmp_pos-1)-signpost
		> SPT_VEC_SIGNPOST_MASK) {
		vecid_s = vec_alloc_from_grp(pclst, 0, &pvec_s);
		if (pvec_s == 0) {
			spt_print("\r\n%d\t%s", __LINE__, __func__);
			spt_set_data_not_free(pdh);
            db_free(pclst, dataid);
            vec_free(pclst, vecid_a);
            vec_free(pclst, vecid_b);
			return SPT_NOMEM;
		}
		pvec_s->val = 0;
		pvec_s->idx = (pinsert->cmp_pos-1)>>SPT_VEC_SIGNPOST_BIT;
		pvec_s->rd = vecid_a;

		pvec_a->down = vecid_b;

		tmp_vec.rd = vecid_s;

	} else if ((pinsert->fs-1)-signpost
		> SPT_VEC_SIGNPOST_MASK) {
		vecid_s = vec_alloc_from_grp(pclst, 0, &pvec_s);
		if (pvec_s == 0) {
			spt_print("\r\n%d\t%s", __LINE__, __func__);
			spt_set_data_not_free(pdh);
            db_free(pclst, dataid);
            vec_free(pclst, vecid_a);
            vec_free(pclst, vecid_b);
			return SPT_NOMEM;
		}
		pvec_s->val = 0;
		pvec_s->idx = (pinsert->fs-1)>>SPT_VEC_SIGNPOST_BIT;
		pvec_s->rd = vecid_b;

		pvec_a->down = vecid_s;

		tmp_vec.rd = vecid_a;

	} else {
		pvec_a->down = vecid_b;

		tmp_vec.rd = vecid_a;
		if (tmp_vec.pos == pvec_a->pos && tmp_vec.pos != 0)
			spt_assert(0);
	}
	if (!pclst->is_bottom) {
		pdh_ext = (struct spt_dh_ext *)pdh->pdata;
		pdh_ext->hang_vec = vecid_a;
    }
	smp_mb();/* ^^^ */
	if (pinsert->key_val == atomic64_cmpxchg(
		(atomic64_t *)pinsert->pkey_vec,
		pinsert->key_val, tmp_vec.val))
        pinsert->hang_vec = vecid_a;
		return dataid;

	spt_set_data_not_free(pdh);
    db_free(pclst, dataid);
    vec_free(pclst, vecid_a);
    vec_free(pclst, vecid_b);
	
	cnt = 3;
	if (pvec_s != 0) {
		vec_free(pclst, vecid_s);
		cnt++;
	}
	#if 0
	ret = fill_in_rsv_list(pclst, cnt, g_thrd_id);
	if (ret == SPT_OK)
		return SPT_DO_AGAIN;
	#endif
	return ret;
}
/**
 * on the comparison path, the last vector's down vector is null,
 * the new data's next bit is zero, insert it below the down vector
 */
int do_insert_last_down(struct cluster_head_t *pclst,
	struct insert_info_t *pinsert,
	char *new_data)
{
	struct spt_vec tmp_vec, *pvec_a, *pvec_s;
	u64 signpost;
	u32 dataid, vecid_a, vecid_s, cnt;
	struct spt_dh *pdh;
	struct spt_dh_ext *pdh_ext;
	int ret;
	int id;

	if(pinsert->alloc_type == SPT_TOP_INSERT)
		id = pclst->last_alloc_id;
	else
		id = pinsert->key_id;
	pvec_s = 0;
    dataid = db_alloc_from_grp(pclst, id, &pdh);
	if (pdh == 0) {
		spt_print("\r\n%d\t%s", __LINE__, __func__);
		return SPT_NOMEM;
	}
	pdh->ref = pinsert->ref_cnt;
	pdh->pdata = new_data;

	tmp_vec.val = pinsert->key_val;
	signpost = pinsert->signpost;

    vecid_a = vec_alloc_from_grp(pclst, id, &pvec_a);
	if (pvec_a == 0) {
		spt_print("\r\n%d\t%s", __LINE__, __func__);
		spt_set_data_not_free(pdh);
        db_free(pclst, dataid);
		return SPT_NOMEM;
	}
	pvec_a->val = 0;
	pvec_a->type = SPT_VEC_DATA;
	pvec_a->pos = (pinsert->fs-1)&SPT_VEC_SIGNPOST_MASK;
	pvec_a->rd = dataid;
	pvec_a->down = SPT_NULL;

	if ((pinsert->fs-1)-signpost
		> SPT_VEC_SIGNPOST_MASK) {
		vecid_s = vec_alloc_from_grp(pclst, 0, &pvec_s);
		if (pvec_s == 0) {
			spt_print("\r\n%d\t%s", __LINE__, __func__);
			spt_set_data_not_free(pdh);
            db_free(pclst, dataid);
            vec_free(pclst, vecid_a);
			return SPT_NOMEM;
		}
		pvec_s->val = 0;
		pvec_s->idx = (pinsert->fs-1)>>SPT_VEC_SIGNPOST_BIT;
		pvec_s->rd = vecid_a;

		tmp_vec.down = vecid_s;
	} else
		tmp_vec.down = vecid_a;
	if (!pclst->is_bottom) {
		pdh_ext = (struct spt_dh_ext *)pdh->pdata;
		pdh_ext->hang_vec = pinsert->key_id;
	}
	smp_mb();/* ^^^ */
	if (pinsert->key_val == atomic64_cmpxchg(
		(atomic64_t *)pinsert->pkey_vec,
		pinsert->key_val, tmp_vec.val))
		return dataid;

	spt_set_data_not_free(pdh);
    db_free(pclst, dataid);
    vec_free(pclst, vecid_a);
	cnt = 2;
	if (pvec_s != 0) {
		vec_free(pclst, vecid_s);
		cnt++;
	}
	#if 0
	ret = fill_in_rsv_list(pclst, cnt, g_thrd_id);
	if (ret == SPT_OK)
		return SPT_DO_AGAIN;
	#endif
	return ret;
}
/**
 * compare with the data corresponding to a down vector,
 * the new data is greater, so insert it above the down vector
 */
int do_insert_up_via_d(struct cluster_head_t *pclst,
	struct insert_info_t *pinsert,
	char *new_data)
{
	struct spt_vec tmp_vec, *pvec_a, *pvec_s, *pvec_down;
	u64 signpost;
	u32 dataid, down_dataid, vecid_a, vecid_s, cnt;
	struct spt_dh *pdh;
	struct spt_dh_ext *pdh_ext;
	int ret;
	int id;

	if(pinsert->alloc_type == SPT_TOP_INSERT)
		id = pclst->last_alloc_id;
	else
		id = pinsert->key_id;
	pvec_s = 0;
    dataid = db_alloc_from_grp(pclst, id, &pdh);
	if (pdh == 0) {
		spt_print("\r\n%d\t%s", __LINE__, __func__);
		return SPT_NOMEM;
	}
	pdh->ref = pinsert->ref_cnt;
	pdh->pdata = new_data;

	tmp_vec.val = pinsert->key_val;
	signpost = pinsert->signpost;

    vecid_a = vec_alloc_from_grp(pclst, id, &pvec_a);
	if (pvec_a == 0) {
		spt_print("\r\n%d\t%s", __LINE__, __func__);
		spt_set_data_not_free(pdh);
        db_free(pclst, dataid);
		return SPT_NOMEM;
	}
	pvec_a->val = 0;
	pvec_a->type = SPT_VEC_DATA;
	pvec_a->pos = (pinsert->fs-1)&SPT_VEC_SIGNPOST_MASK;
	pvec_a->rd = dataid;

	if ((pinsert->fs-1)-signpost > SPT_VEC_SIGNPOST_MASK) {
		vecid_s = vec_alloc_from_grp(pclst, 0, &pvec_s);
		if (pvec_s == 0) {
			spt_print("\r\n%d\t%s", __LINE__, __func__);
			spt_set_data_not_free(pdh);
            db_free(pclst, dataid);
            vec_free(pclst, vecid_a);
			return SPT_NOMEM;
		}
		pvec_s->val = 0;
		pvec_s->idx = (pinsert->fs-1)>>SPT_VEC_SIGNPOST_BIT;
		pvec_s->rd = vecid_a;

		pvec_a->down = tmp_vec.down;
		tmp_vec.down = vecid_s;
	} else {
		pvec_a->down = tmp_vec.down;
		tmp_vec.down = vecid_a;
	}
	if (!pclst->is_bottom) {
		pdh_ext = (struct spt_dh_ext *)pdh->pdata;
		pdh_ext->hang_vec = pinsert->key_id;
	}
	smp_mb();/* ^^^ */
	if (pinsert->key_val == atomic64_cmpxchg(
				(atomic64_t *)pinsert->pkey_vec,
				pinsert->key_val, tmp_vec.val)) {
		if (!pclst->is_bottom) {
			pvec_down = (struct spt_vec *)vec_id_2_ptr(pclst,
				pvec_a->down);
			down_dataid = get_data_id(pclst, pvec_down);
			if (down_dataid < 0) {
				spt_debug("get_data_id error\r\n");
				spt_assert(0);
			}
			pdh = (struct spt_dh *)db_id_2_ptr(pclst, down_dataid);
			pdh_ext = (struct spt_dh_ext *)pdh->pdata;
			pdh_ext->hang_vec = vecid_a;
		}
		return dataid;
	}

	spt_set_data_not_free(pdh);
    db_free(pclst, dataid);
    vec_free(pclst, vecid_a);
	cnt = 2;
	if (pvec_s != 0) {
		vec_free(pclst, vecid_s);
		cnt++;
	}
#if 0
	ret = fill_in_rsv_list(pclst, cnt, g_thrd_id);
	if (ret == SPT_OK)
		return SPT_DO_AGAIN;
	#endif
	return ret;
}
/**
 * only top level cluster will maintain hang vector
 * only divided thread will operate top level cluster
 */
void refresh_db_hang_vec(struct cluster_head_t *pclst,
	struct spt_vec *pdata_vec,
	struct spt_dh *pdel_dh)
{
	int down_data;
	struct spt_vec *pvec;
	struct spt_dh *pdown_dh;
	struct spt_dh_ext *pext_h, *pdown_ext_h;

	if (pdata_vec->down == SPT_NULL)
		return;
	pvec = (struct spt_vec *)vec_id_2_ptr(pclst, pdata_vec->down);
	while (pvec->type != SPT_VEC_DATA)
		pvec = (struct spt_vec *)vec_id_2_ptr(pclst, pvec->rd);
	down_data = pvec->rd;
	pdown_dh = (struct spt_dh *)db_id_2_ptr(pclst, down_data);
	pdown_ext_h = (struct spt_dh_ext *)pdown_dh->pdata;
	pext_h = (struct spt_dh_ext *)pdel_dh->pdata;
	pdown_ext_h->hang_vec = pext_h->hang_vec;
}
/**
 * find_lowest_data - get the data id of the bottom from @start_vec
 * @pclst: pointer of sd tree cluster head
 * @pvec: pointer of a vector in the cluster
 *
 * when traversing down, deal with the invalid vector
 * ret db_id may be deleted
 */
int find_lowest_data_slow(struct cluster_head_t *pclst, struct spt_vec *pvec)
{
	struct spt_vec *pcur, *pnext, *ppre;
	struct spt_vec tmp_vec, cur_vec, next_vec;
	//u8 direction;
	u32 vecid;
	//int ret;

find_lowest_start:
	ppre = 0;
	cur_vec.val = pvec->val;
	pcur = pvec;
	if (cur_vec.status == SPT_VEC_RAW) {
		smp_mb();/* ^^^ */
		cur_vec.val = pvec->val;
		if (cur_vec.status == SPT_VEC_RAW)
			return SPT_DO_AGAIN;
	}
	if (cur_vec.status == SPT_VEC_INVALID)
		return SPT_DO_AGAIN;

	while (1) {
		if (cur_vec.type == SPT_VEC_DATA
			&& cur_vec.down == SPT_NULL)
			return cur_vec.rd;

		if (cur_vec.down != SPT_NULL) {
			pnext = (struct spt_vec *)vec_id_2_ptr(pclst,
					cur_vec.down);
			next_vec.val = pnext->val;
			if (next_vec.status == SPT_VEC_RAW) {
				smp_mb();/* ^^^ */
				next_vec.val = pnext->val;
				if (next_vec.status == SPT_VEC_RAW)
					goto find_lowest_start;
			}
			if (next_vec.status == SPT_VEC_INVALID) {
				tmp_vec.val = cur_vec.val;
				vecid = cur_vec.down;

				tmp_vec.down = next_vec.down;
				if (next_vec.type != SPT_VEC_DATA
					|| (next_vec.type == SPT_VEC_DATA
						&& next_vec.rd != SPT_NULL)) {
					struct spt_vec tmp_vec_b;

					tmp_vec_b.val = next_vec.val;
					tmp_vec_b.status = SPT_VEC_VALID;
					atomic64_cmpxchg((atomic64_t *)pnext,
						next_vec.val, tmp_vec_b.val);
//set invalid succ or not, refind from cur
					cur_vec.val = pcur->val;
					if (cur_vec.status == SPT_VEC_INVALID)
						goto find_lowest_start;
					continue;
				}
						//BUG();
				if (cur_vec.val == atomic64_cmpxchg(
					(atomic64_t *)pcur,
					cur_vec.val,
					tmp_vec.val))//delete succ
					vec_free(pclst,
					vecid);

				cur_vec.val = pcur->val;
				if (cur_vec.status != SPT_VEC_VALID)
					goto find_lowest_start;
				continue;
			}
		} else {
		// cur_vec.type == SPT_VEC_RIGHT
			pnext = (struct spt_vec *)vec_id_2_ptr(pclst,
				cur_vec.rd);
			next_vec.val = pnext->val;
			if (next_vec.status == SPT_VEC_RAW) {
				smp_mb();/* ^^^ */
				next_vec.val = pnext->val;
				if (next_vec.status == SPT_VEC_RAW)
					goto find_lowest_start;
			}
			if (next_vec.status == SPT_VEC_INVALID) {
				tmp_vec.val = cur_vec.val;
				vecid = cur_vec.rd;
				tmp_vec.rd = next_vec.rd;

				if (next_vec.type == SPT_VEC_DATA
					&& next_vec.down == SPT_NULL) {
					spt_set_data_flag(tmp_vec);
					if (next_vec.rd == SPT_NULL
						&& pcur != pclst->pstart)
						tmp_vec.status
						= SPT_VEC_INVALID;
				}
				if (next_vec.type == SPT_VEC_DATA
					&& next_vec.down != SPT_NULL)
					tmp_vec.rd = next_vec.down;

				if (cur_vec.val == atomic64_cmpxchg(
					(atomic64_t *)pcur,
					cur_vec.val, tmp_vec.val)) {
                       vec_free(pclst, vecid);
				}
				cur_vec.val = pcur->val;
				if (cur_vec.status != SPT_VEC_VALID)
					goto find_lowest_start;
				continue;
			}

		}
		ppre = pcur;
		pcur = pnext;
		cur_vec.val = next_vec.val;
	}
}

/**
 * find_lowest_data - get the data id of the bottom from @start_vec
 * @pclst: pointer of sd tree cluster head
 * @pvec: pointer of a vector in the cluster
 *
 * when traversing down, don't deal with the invalid vector
 * ret db_id may be deleted
 */
int find_lowest_data(struct cluster_head_t *pclst,
	struct spt_vec *start_vec)
{
	struct spt_vec *pcur, cur_vec;

	pcur = start_vec;
	cur_vec.val = pcur->val;

	while (1) {
		if (cur_vec.type == SPT_VEC_DATA) {
			if (cur_vec.down == SPT_NULL)
				return cur_vec.rd;

			pcur = (struct spt_vec *)vec_id_2_ptr(pclst,
					cur_vec.down);
			cur_vec.val = pcur->val;
		} else if (cur_vec.type == SPT_VEC_RIGHT) {
			if (cur_vec.down == SPT_NULL)
				pcur = (struct spt_vec *)vec_id_2_ptr(pclst,
					cur_vec.rd);
			else
				pcur = (struct spt_vec *)vec_id_2_ptr(pclst,
					cur_vec.down);
			cur_vec.val = pcur->val;
		} else {
			spt_debug("type error\r\n");
			spt_assert(0);
		}
	}
}
/*nth>=1, divided thread call it to choose one data to construct virtual board*/
char *get_about_Nth_smallest_data(struct spt_sort_info *psort, int nth)
{
	int idx = psort->idx;

	if (psort->array[idx] == NULL) {
		if (nth >= idx)
			return psort->array[0];
		return psort->array[idx-nth];
	}

	if (nth >= psort->size)
		return psort->array[idx];
	return psort->array[(idx+psort->size-nth)%psort->size];
}
void spt_order_array_free(struct spt_sort_info *psort)
{
	spt_vfree(psort);
}
struct spt_sort_info *spt_order_array_init(struct cluster_head_t *pclst,
		int size)
{
	struct spt_sort_info *psort_ar;

	psort_ar = (struct spt_sort_info *)spt_vmalloc(
		sizeof(struct spt_sort_info) + sizeof(char *)*size);
	if (psort_ar == NULL)
		return NULL;
	psort_ar->idx = 0;
	psort_ar->cnt = 0;
	psort_ar->size = size;
	return psort_ar;
}
int spt_cluster_sort(struct cluster_head_t *pclst, struct spt_sort_info *psort)
{
	struct spt_vec **stack;
	int cur_data, cur_vecid, index;
	struct spt_vec *pcur, cur_vec;
//	struct spt_vec_f st_vec_f;
	struct spt_dh *pdh;

	stack = (struct spt_vec **)spt_malloc(4096*8*8);
	if (stack == NULL)
		return SPT_ERR;
	index = 0;
	cur_data = SPT_INVALID;

	cur_vecid = pclst->vec_head;
	pcur = (struct spt_vec *)vec_id_2_ptr(pclst, pclst->vec_head);

	cur_vec.val = pcur->val;
	if (cur_vec.down == SPT_NULL && cur_vec.rd == SPT_NULL) {
		spt_print("cluster is null\r\n");
		return SPT_ERR;
	}
	stack[index] = pcur;
	index++;

	while (1) {
		if (cur_vec.type != SPT_VEC_DATA) {
			cur_vecid = cur_vec.rd;
			pcur = (struct spt_vec *)vec_id_2_ptr(pclst, cur_vecid);
			cur_vec.val = pcur->val;
			stack[index] = pcur;
			index++;
		} else {
			cur_data = cur_vec.rd;
			if (cur_data != SPT_NULL) {
				pdh = (struct spt_dh *)db_id_2_ptr(pclst,
						cur_data);
				psort->array[psort->idx] = pdh->pdata;
				psort->idx = (psort->idx+1)%psort->size;
				psort->cnt++;
				//debug_pdh_data_print(pclst, pdh);
			}

			if (index == 0)
				break;
			while (1) {
				index--;
				pcur = stack[index];
				cur_vec.val = pcur->val;
				if (cur_vec.down != SPT_NULL) {
					cur_vecid = cur_vec.down;
					pcur = (struct spt_vec *)
						vec_id_2_ptr(pclst, cur_vecid);
					cur_vec.val = pcur->val;

					stack[index] = pcur;
					index++;
					break;
				}
				if (index == 0)
					goto sort_exit;
			}
		}
	}
sort_exit:
	spt_free(stack);
	return SPT_OK;
}

void spt_divided_info_free(struct spt_divided_info *pdvd_info)
{
	int loop;
	char **vb_array;
	struct cluster_head_t *pclst;

	if (pdvd_info->up_vb_arr) {
		vb_array = pdvd_info->up_vb_arr;
		pclst = pdvd_info->puclst;
		for (loop = 0; loop < pdvd_info->divided_times; loop++) {
			if (vb_array[loop])
				pclst->freedata(vb_array[loop]);
		}
		spt_vfree(pdvd_info->up_vb_arr);
	}
	if (pdvd_info->down_vb_arr) {
		vb_array = pdvd_info->down_vb_arr;
		pclst = pdvd_info->pdst_clst;
		for (loop = 0; loop < pdvd_info->divided_times; loop++) {
			if (vb_array[loop])
				pclst->freedata(vb_array[loop]);
		}
		spt_vfree(pdvd_info->down_vb_arr);
	}
	spt_vfree(pdvd_info);
}
struct spt_divided_info *spt_divided_mem_init(int dvd_times,
		struct cluster_head_t *pdclst)
{
	struct spt_divided_info *pdvd_info;

	pdvd_info = (struct spt_divided_info *)
		spt_vmalloc(sizeof(struct spt_divided_info));
	memset(pdvd_info, 0, sizeof(*pdvd_info));
	pdvd_info->up_vb_arr = (char **)spt_vmalloc(sizeof(char *)*dvd_times);
	if (pdvd_info->up_vb_arr == NULL) {
		spt_divided_info_free(pdvd_info);
		return NULL;
	}
	memset(pdvd_info->up_vb_arr, 0, sizeof(char *)*dvd_times);
	pdvd_info->down_vb_arr = (char **)spt_vmalloc(sizeof(char *)*dvd_times);
	if (pdvd_info->down_vb_arr == NULL) {
		spt_divided_info_free(pdvd_info);
		return NULL;
	}
	memset(pdvd_info->down_vb_arr, 0, sizeof(char *)*dvd_times);
	pdvd_info->divided_times = dvd_times;
	pdvd_info->down_is_bottom = pdclst->is_bottom;

	pdvd_info->pdst_clst =
		cluster_init(pdvd_info->down_is_bottom,
						pdclst->startbit,
						pdclst->endbit,
						pdclst->thrd_total,
						pdclst->get_key,
						pdclst->get_key_end,
						pdclst->freedata,
						pdclst->construct_data);
	if (pdvd_info->pdst_clst == NULL) {
		spt_divided_info_free(pdvd_info);
		return NULL;
	}
	return pdvd_info;
}

int spt_divided_info_init(struct spt_divided_info *pdvd_info,
	struct spt_sort_info *psort,
	struct cluster_head_t *puclst)
{
	int loop, n;
	char **u_vb_array, **d_vb_array;
	char *pdata, *psrc, *pNth_data;

	pdvd_info->puclst = puclst;

	u_vb_array = pdvd_info->up_vb_arr;
	d_vb_array = pdvd_info->down_vb_arr;

	n = 0;
	for (loop = 0; loop < pdvd_info->divided_times; loop++) {
		n += SPT_DVD_CNT_PER_TIME;
		pNth_data = get_about_Nth_smallest_data(psort, n);
		psrc = pdvd_info->pdst_clst->get_key_in_tree(pNth_data);

		pdata = puclst->construct_data(psrc);
		if (pdata == NULL)
			return SPT_ERR;
		u_vb_array[loop] = pdata;

		pdata = pdvd_info->pdst_clst->construct_data(psrc);
		if (pdata == NULL)
			return SPT_ERR;
		d_vb_array[loop] = pdata;
		pdvd_info->pdst_clst->get_key_in_tree_end(psrc);
	}
	return SPT_OK;
}
/**
 * divide_sub_cluster - divide a cluster into two
 * @pclst: pointer of sd tree cluster head
 * @pdata: pointer of data head in the up level cluster
 *
 * When the number of data in a cluster exceeds SPT_DVD_THRESHOLD_VA
 * divide the cluster into two
 */
int divide_sub_cluster(struct cluster_head_t *pclst, struct spt_dh_ext *pup)
{
	int loop, dataid, ins_dvb_id, ret, total, sched, ref_cnt;
	int move_time = SPT_DVD_MOVE_TIMES;
//	int move_per_cnt = 100;
	struct spt_sort_info *psort;
	struct spt_divided_info *pdinfo;
	struct spt_dh_ext *pext_head;
	struct cluster_head_t *plower_clst, *pdst_clst;
	struct spt_dh *pdh;
	struct query_info_t qinfo = {0};
	u64 start;
	u64 w_start, w_total, w_cnt;
	u64 f_start, f_total, f_cnt;
	u64 del_start, del_total, del_cnt;
	u64 ins_start, ins_total, ins_cnt;
	u64 sort_start, sort_total;

	w_start = w_total = w_cnt = 0;
	f_start = f_total = f_cnt = 0;
	del_start = del_total = del_cnt = 0;
	ins_start = ins_total = ins_cnt = 0;

	pext_head = pup;
	plower_clst = pext_head->plower_clst;

	pdinfo = spt_divided_mem_init(move_time, pext_head->plower_clst);
	if (pdinfo == NULL) {
		spt_debug("spt_divided_mem_init return NULL\r\n");
		return SPT_ERR;
	}
	psort = spt_order_array_init(pext_head->plower_clst,
		pext_head->plower_clst->data_total);
	if (psort == NULL) {
		spt_debug("spt_order_array_init return NULL\r\n");
		cluster_destroy(pdinfo->pdst_clst);
		spt_divided_info_free(pdinfo);
		return SPT_ERR;
	}
	plower_clst->ins_mask = 1;
	spt_preempt_disable();
	spt_thread_start(g_thrd_id);

	sort_start = rdtsc();
	/* sort the data in the cluster.move the smaller half of the data to
	 * the new cluster later
	 */
	ret = spt_cluster_sort(pext_head->plower_clst, psort);
	if (ret == SPT_ERR) {
		spt_thread_exit(g_thrd_id);
		spt_preempt_enable();
		spt_debug("spt_cluster_sort return ERR\r\n");
		cluster_destroy(pdinfo->pdst_clst);
		spt_divided_info_free(pdinfo);
		spt_order_array_free(psort);
		return SPT_ERR;
	}
	ret = spt_divided_info_init(pdinfo, psort, pclst);
	sort_total = rdtsc()-sort_start;
	spt_thread_exit(g_thrd_id);
	spt_preempt_enable();
	spt_print("dvd sort cycle:%llu\r\n", sort_total);
	if (ret != SPT_OK) {
		spt_debug("spt_divided_info_init return ERR\r\n");
		cluster_destroy(pdinfo->pdst_clst);
		spt_divided_info_free(pdinfo);
		return SPT_ERR;
	}

	pdst_clst = pdinfo->pdst_clst;

	for (loop = 0; loop < move_time; loop++) {
		spt_preempt_disable();
		spt_thread_start(g_thrd_id);
		/* insert the virtual board into the up-level cluster
		 * if new insert/delete/find match this board, it is masked
		 */
		do_insert_data(pclst, pdinfo->up_vb_arr[loop],
						pclst->get_key_in_tree,
						pclst->get_key_in_tree_end);
		spt_thread_exit(g_thrd_id);
		/* after 2 ticks, query falling into the virtual
		 * board scope must have been completed
		 */
		spt_thread_wait(2, g_thrd_id);
		spt_thread_start(g_thrd_id);

		/* insert a virtual board into the low-level cluster*/
		qinfo.op = SPT_OP_INSERT;
        //qinfo.signpost = 0;
		qinfo.pstart_vec = plower_clst->pstart;
		qinfo.startid = plower_clst->vec_head;
		qinfo.endbit = plower_clst->endbit;
		qinfo.data = pdinfo->down_vb_arr[loop];
		qinfo.multiple = 1;
		qinfo.get_key = plower_clst->get_key;
		if (find_data(plower_clst, &qinfo) != SPT_OK)
			spt_assert(0);
		ins_dvb_id = qinfo.db_id;
		total = 0;
		sched = 0;
		/* move the datas below the virtual board to the new cluster */
		while (1) {
			f_start = rdtsc();
			dataid = find_lowest_data(plower_clst,
				plower_clst->pstart);
			if (dataid == SPT_NULL)
				dataid = find_lowest_data_slow(plower_clst,
				plower_clst->pstart);
			f_total += rdtsc()-f_start;
			f_cnt++;
			pdh = (struct spt_dh *)db_id_2_ptr(plower_clst, dataid);
			ref_cnt = pdh->ref;
			start = rdtsc();
			while (1) {
				del_start = rdtsc();
				ret = do_delete_data_no_free_multiple(
					plower_clst,
					pdh->pdata,
					ref_cnt,
					plower_clst->get_key_in_tree,
					plower_clst->get_key_in_tree_end);
				if (ret == SPT_OK) {
					del_total += rdtsc()-del_start;
					del_cnt++;
					break;
				} else if (ret == SPT_WAIT_AMT) {
					spt_thread_exit(g_thrd_id);
					w_start = rdtsc();
					spt_thread_wait(2, g_thrd_id);
					w_total += rdtsc() - w_start;
					w_cnt++;
					spt_thread_start(g_thrd_id);
				} else
					spt_debug("divide delete error\r\n");
			}
			if (dataid == ins_dvb_id) {
				ref_cnt--;
				if (ref_cnt == 0)
					break;
			}
			start = rdtsc();

			ret = do_insert_data_multiple(pdst_clst,
				pdh->pdata,
				ref_cnt,
				pdst_clst->get_key_in_tree,
				pdst_clst->get_key_in_tree_end);
			if (ret != SPT_OK)
				spt_debug("divide insert error\r\n");
			ins_total += rdtsc()-start;
			ins_cnt++;
			if (dataid == ins_dvb_id)
				break;
		}
		pext_head = (struct spt_dh_ext *)pdinfo->up_vb_arr[loop];
		pext_head->plower_clst = pdst_clst;
		if (1) {
			spt_thread_exit(g_thrd_id);
			spt_preempt_enable();
			spt_schedule();
			spt_preempt_disable();
			spt_thread_start(g_thrd_id);
		}
		if (loop > 0) {
			while (1) {
				ret = do_delete_data(pclst,
					pdinfo->up_vb_arr[loop-1],
					pclst->get_key_in_tree,
					pclst->get_key_in_tree_end);
				if (ret == SPT_OK)
					break;
				else if (ret == SPT_WAIT_AMT) {
					spt_thread_exit(g_thrd_id);
					w_start = rdtsc();
					spt_thread_wait(2, g_thrd_id);
					w_total += rdtsc() - w_start;
					w_cnt++;
					spt_preempt_enable();
					spt_schedule();
					spt_preempt_disable();
					spt_thread_start(g_thrd_id);
				} else
					spt_debug("divide delete error\r\n");
			}
			pdinfo->up_vb_arr[loop-1] = 0;
		}
		spt_thread_exit(g_thrd_id);
		spt_preempt_enable();
	}
	pdinfo->up_vb_arr[loop-1] = 0;
	list_add(&pdst_clst->c_list, &pclst->c_list);
	spt_divided_info_free(pdinfo);
	spt_order_array_free(psort);
	plower_clst->ins_mask = 0;
	spt_print("================dvd cycle info================\r\n");
	spt_print("wait 2 tick %llu times, av cycle:%llu\r\n",
		w_cnt, (w_cnt != 0?w_total/w_cnt:0));
	spt_print("find lowest %llu times, av cycle:%llu\r\n",
		f_cnt, (f_cnt != 0?f_total/f_cnt:0));
	spt_print("delete succ %llu times, av cycle:%llu\r\n",
		del_cnt, (del_cnt != 0?del_total/del_cnt:0));
	spt_print("insert succ %llu times, av cycle:%llu\r\n",
		ins_cnt, (ins_cnt != 0?ins_total/ins_cnt:0));
	spt_print("==============================================\r\n");

	return SPT_OK;
}
int spt_divided_scan(struct cluster_head_t *pclst)
{
	struct spt_sort_info *psort;
	struct spt_dh_ext *pdh_ext;
	struct cluster_head_t *plower_clst;
	int i, ret;

	psort = spt_order_array_init(pclst, pclst->data_total);
	if (psort == NULL) {
		spt_debug("spt_order_array_init return NULL\r\n");
		return SPT_ERR;
	}

	ret = spt_cluster_sort(pclst, psort);
	if (ret == SPT_ERR) {
		spt_debug("psort ERR\r\n");
		return SPT_ERR;
	}
	for (i = 0; i < psort->size; i++) {
		pdh_ext = (struct spt_dh_ext *)psort->array[i];
		plower_clst = pdh_ext->plower_clst;
		if (plower_clst->data_total >= SPT_DVD_THRESHOLD_VA)
			divide_sub_cluster(pclst, pdh_ext);
	}
	spt_order_array_free(psort);
	return SPT_OK;
}
//#define DEBUG_FIND_PATH
#ifdef DEBUG_FIND_PATH
struct debug_find_path {
	char *page;
	int startbit;
	int len;
	int direct;
};

struct debug_find_path debug_path[48][1024] = {{{0} } };
int path_index[48] = {0};
unsigned char path_data[48][4096] = {{0} };


void print_debug_path(int id)
{
	unsigned char *pfind;
	int i;

	pfind = path_data[id];
	spt_print("=======path_data:=======\r\n");
	for (i = 0; i < 4096; i++) {
		if (i%32 == 0)
			spt_print("\r\n");
		spt_print("%02x ", *((unsigned char *)pfind + i));
	}
	spt_print("=======find_path:=======\r\n");
	for (i = 0; i < path_index[id] && i < 1024; i++) {
		spt_print("page:%p startbit:%d len:%d direct:%d\r\n",
			debug_path[id][i].page,
			debug_path[id][i].startbit,
			debug_path[id][i].len,
			debug_path[id][i].direct);
	}
}
#endif

/*ret:1 not found;0 successful; -1 error*/
/**
 * find_data - data find/delete/insert in a sd tree cluster
 * @pclst: pointer of sd tree cluster head
 * @query_info_t: pointer of data query info
 * return 1 not found;0 successful; < 0 error
 */
int find_data(struct cluster_head_t *pclst, struct query_info_t *pqinfo)
{
	int cur_data, vecid, cmp, op, cur_vecid, pre_vecid, next_vecid, cnt;
	struct spt_vec *pcur, *pnext, *ppre;
	struct spt_vec tmp_vec, cur_vec, next_vec;
	char *pcur_data;
	u64 startbit, endbit, len, fs_pos, signpost;
	int va_old, va_new;
	u8 direction;
	int ret;
	int retb;
	struct vec_cmpret_t cmpres;
	struct insert_info_t st_insert_info = { 0};
	char *pdata, *prdata;
	struct spt_dh *pdh;
//	spt_cb_get_key get_key;
	spt_cb_end_key finish_key_cb;

	if (pclst->status == SPT_WAIT_AMT) {
		//cnt = rsv_list_fill_cnt(pclst, g_thrd_id);
		//ret = fill_in_rsv_list(pclst, cnt, g_thrd_id);
		if (ret == SPT_OK)
			pclst->status = SPT_OK;
		else
			return ret;
	}
	ret = SPT_NOT_FOUND;
	op = pqinfo->op;

	pdata = pqinfo->data;

	if (pqinfo->get_key == NULL)
		prdata = pclst->get_key(pqinfo->data);
	else
		prdata = pqinfo->get_key(pqinfo->data);
	if (pqinfo->get_key_end == NULL)
		finish_key_cb = pclst->get_key_end;
	else
		finish_key_cb = pqinfo->get_key_end;

	cur_data = SPT_INVALID;

refind_start:
	signpost = pqinfo->signpost;
	pcur = pqinfo->pstart_vec;
	cur_vecid = pre_vecid = pqinfo->startid;
#ifdef DEBUG_FIND_PATH
	if (op != SPT_OP_FIND)
		path_index[g_thrd_id] = 0;
#endif
refind_forward:

	if (pcur == NULL)
		goto refind_start;
	ppre = NULL;
	cur_vecid = pre_vecid;
	pre_vecid = SPT_INVALID;
	cur_vec.val = pcur->val;
	if (pcur == pclst->pstart) {
		startbit = pclst->startbit;
		#ifdef DEBUG_FIND_PATH
		if (op != SPT_OP_FIND)
			path_index[g_thrd_id] = 0;
		#endif
	} else {
		startbit = signpost + cur_vec.pos + 1;
		#ifdef DEBUG_FIND_PATH
		if (op != SPT_OP_FIND)
			path_index[g_thrd_id]--;
		#endif
	}
	endbit = pqinfo->endbit;
	if (cur_vec.status == SPT_VEC_RAW) {
		smp_mb();/* ^^^ */
		cur_vec.val = pcur->val;
		if (cur_vec.status == SPT_VEC_RAW) {
			if (pcur == pqinfo->pstart_vec) {
				finish_key_cb(prdata);
				printf("return do agin line %d\r\n",__LINE__);
				return SPT_DO_AGAIN;
			}
			goto refind_start;
		}
	}
	if (cur_vec.status == SPT_VEC_INVALID
		|| cur_vec.type == SPT_VEC_SIGNPOST) {
		if (pcur == pqinfo->pstart_vec) {
			finish_key_cb(prdata);
			return SPT_DO_AGAIN;
		}
		goto refind_start;
	}
	direction = SPT_DIR_START;
	if (cur_data != SPT_INVALID) {
		cur_data = SPT_INVALID;
		pclst->get_key_in_tree_end(pcur_data);
	}

	fs_pos = find_fs(prdata, startbit, endbit-startbit);
	while (startbit < endbit) {
		/*first bit is 1£¬compare with pcur_vec->right*/
		if (fs_pos != startbit)
			goto go_down;
go_right:
		if (cur_vec.type == SPT_VEC_DATA) {
			len = endbit - startbit;
			if (cur_data != SPT_INVALID) {
				pclst->get_key_in_tree_end(pcur_data);
				if (cur_vec.rd != cur_data) {
					cur_data = SPT_INVALID;
					goto refind_start;
				}
			}
			cur_data = cur_vec.rd;
			if (cur_data >= 0 && cur_data < SPT_INVALID) {
				pdh = (struct spt_dh *)db_id_2_ptr(pclst,
						cur_data);
				smp_mb();/* ^^^ */
				pcur_data = pclst->get_key_in_tree(pdh->pdata);
			} else if (cur_data == SPT_NULL) {
				switch (op) {
				case SPT_OP_FIND:
					finish_key_cb(prdata);
					return ret;
				case SPT_OP_INSERT:
					st_insert_info.pkey_vec = pcur;
					st_insert_info.key_val = cur_vec.val;
					st_insert_info.ref_cnt =
						pqinfo->multiple;
					ret = do_insert_first_set(
							pclst,
							&st_insert_info,
							pdata);
					if (ret == SPT_DO_AGAIN) {
						cur_data = SPT_INVALID;
						goto refind_start;
					} else if (ret >= 0) {
						pqinfo->db_id = ret;
						pqinfo->data = 0;
						finish_key_cb(prdata);
						return SPT_OK;
					}
					finish_key_cb(prdata);
					return ret;

					break;
				case SPT_OP_DELETE:
					finish_key_cb(prdata);
					return ret;
				default:
					break;
				}
			} else
				spt_assert(0);
			//ppre = pcur;
			//pre_vec.val = cur_vec.val;
			//pcur = NULL;
		} else {
			pnext = (struct spt_vec *)vec_id_2_ptr(pclst,
					cur_vec.rd);
			next_vec.val = pnext->val;
			next_vecid = cur_vec.rd;
			if (next_vec.status == SPT_VEC_RAW) {
				smp_mb();/* ^^^ */
				next_vec.val = pnext->val;
				if (next_vec.status == SPT_VEC_RAW)
					goto refind_start;
			}
			if (next_vec.status == SPT_VEC_INVALID) {
				tmp_vec.val = cur_vec.val;
				vecid = cur_vec.rd;
				tmp_vec.rd = next_vec.rd;

				if (next_vec.type == SPT_VEC_DATA
					&& next_vec.down == SPT_NULL) {
					spt_set_data_flag(tmp_vec);
					if (next_vec.rd == SPT_NULL
						&& pcur != pclst->pstart)
						tmp_vec.status
						= SPT_VEC_INVALID;
				}
				if (next_vec.type == SPT_VEC_DATA
					&& next_vec.down != SPT_NULL)
					tmp_vec.rd = next_vec.down;

				if (cur_vec.val == atomic64_cmpxchg(
						(atomic64_t *)pcur,
						cur_vec.val,
						tmp_vec.val)) {
					vec_free(pclst, vecid);
					retb = SPT_OK;
						
					if (retb != SPT_OK
						&& cur_data != SPT_INVALID)
						pclst->get_key_in_tree_end(
							pcur_data);
					if (retb != SPT_OK) {
						finish_key_cb(prdata);
						retb =
							(ret == SPT_OK)
							? ret:retb;
						return retb;
					}
				}
				cur_vec.val = pcur->val;
				if (cur_vec.status != SPT_VEC_VALID) {
					pcur = ppre;
					goto refind_forward;
				}
				continue;
			}
			if (next_vec.down == SPT_NULL) {
				tmp_vec.val = next_vec.val;
				tmp_vec.status = SPT_VEC_INVALID;
				atomic64_cmpxchg((atomic64_t *)pnext,
						next_vec.val,
						tmp_vec.val);
				//set invalid succ or not, refind from cur
				cur_vec.val = pcur->val;
				if (cur_vec.status != SPT_VEC_VALID) {
					pcur = ppre;
					goto refind_forward;
				}
				continue;
			}
			len = next_vec.pos + signpost - startbit + 1;

		}
		if (cur_data == SPT_INVALID
			&& cur_vec.type != SPT_VEC_DATA) {
			cur_data = get_data_id(pclst, pnext);
			if (cur_data >= 0
				&& cur_data < SPT_INVALID) {
				pdh = (struct spt_dh *)db_id_2_ptr(pclst,
					cur_data);
				smp_mb();/* ^^^ */
				pcur_data = pclst->get_key_in_tree(
					pdh->pdata);
			} else if (cur_data == SPT_DO_AGAIN) {
				cur_data = SPT_INVALID;
				goto refind_start;
			} else if (cur_data == SPT_NULL) {
				switch (op) {
				case SPT_OP_FIND:
					finish_key_cb(prdata);
					return ret;
				case SPT_OP_INSERT:
					st_insert_info.pkey_vec = pcur;
					st_insert_info.key_val = cur_vec.val;
					st_insert_info.ref_cnt =
						pqinfo->multiple;
					ret = do_insert_first_set(pclst,
						&st_insert_info,
						pdata);
					if (ret == SPT_DO_AGAIN) {
						cur_data = SPT_INVALID;
						goto refind_start;
					} else if (ret >= 0) {
						pqinfo->db_id = ret;
						pqinfo->data = 0;
                        pqinfo->vec_id = cur_vecid;
						finish_key_cb(prdata);
						return SPT_OK;
					}
					finish_key_cb(prdata);
					return ret;
				case SPT_OP_DELETE:
					finish_key_cb(prdata);
					return ret;
				default:
					break;
				}

			} else {
				//SPT_NOMEM or SPT_WAIT_AMT;

				finish_key_cb(prdata);
				if (ret == SPT_OK)
					return ret;
				return cur_data;
			}
		}

		cmp = diff_identify(prdata, pcur_data, startbit, len, &cmpres);

		#ifdef DEBUG_FIND_PATH
		if (op != SPT_OP_FIND) {
			int index;

			index = path_index[g_thrd_id]&0x3ff;
			debug_path[g_thrd_id][index].page = pdh->pdata;
			debug_path[g_thrd_id][index].startbit = startbit;
			debug_path[g_thrd_id][index].len = len;
			debug_path[g_thrd_id][index].direct = 1;
			spt_bit_cpy(path_data[g_thrd_id],
				pcur_data,
				startbit,
				len);
			path_index[g_thrd_id]++;
		}
		#endif
		if (cmp == 0) {
			startbit += len;
			/*find the same record*/
			if (startbit >= endbit)
				break;
			ppre = pcur;
			pcur = pnext;
			pre_vecid = cur_vecid;
			cur_vecid = next_vecid;
			cur_vec.val = next_vec.val;
			direction = SPT_RIGHT;
			///TODO:startbit already >= DATA_BIT_MAX
			fs_pos = find_fs(prdata,
				startbit,
				endbit - startbit);
			continue;
		}
	/*insert up*/
		if (cur_vec.type != SPT_VEC_DATA)
			if (cur_data != get_data_id(pclst, pnext))
				goto refind_start;
		if (cmp > 0) {
			switch (op) {
			case SPT_OP_FIND:
				if (pqinfo->op == SPT_OP_FIND) {
					pqinfo->db_id = cur_data;
					pqinfo->vec_id = cur_vecid;
					pqinfo->cmp_result = 1;
				}

				finish_key_cb(prdata);

				if (cur_data != SPT_INVALID)
					pclst->get_key_in_tree_end(pcur_data);
				return ret;
			case SPT_OP_INSERT:
				st_insert_info.pkey_vec = pcur;
				st_insert_info.key_val = cur_vec.val;
				st_insert_info.cmp_pos = cmpres.pos;
				st_insert_info.fs = cmpres.smallfs;
				st_insert_info.signpost = signpost;
				st_insert_info.endbit = startbit+len;
				st_insert_info.dataid = cur_data;
				//for debug
                //st_insert_info.pcur_data = pcur_data;
                //st_insert_info.startbit = startbit;
                //st_insert_info.cmpres = cmpres;
                st_insert_info.ref_cnt = pqinfo->multiple;
				st_insert_info.alloc_type = pqinfo->res;
				st_insert_info.key_id = cur_vecid;
				ret = do_insert_up_via_r(pclst,
					&st_insert_info,
					pdata);
				if (ret == SPT_DO_AGAIN)
					goto refind_start;
				else if (ret >= 0) {
					pqinfo->db_id = ret;
					pqinfo->data = 0;
                            pqinfo->vec_id = cur_vecid;
					finish_key_cb(prdata);

					if (cur_data != SPT_INVALID)
						pclst->get_key_in_tree_end(
							pcur_data);
					return SPT_OK;
				}
				finish_key_cb(prdata);
				if (cur_data != SPT_INVALID)
					pclst->get_key_in_tree_end(pcur_data);
				return ret;
			case SPT_OP_DELETE:
				finish_key_cb(prdata);

				if (cur_data != SPT_INVALID)
					pclst->get_key_in_tree_end(pcur_data);
				return ret;
			default:
				break;
			}
		} else {
		/*insert down*/
			switch (op) {
			case SPT_OP_FIND:
				if (pqinfo->op == SPT_OP_FIND) {
					pqinfo->db_id = cur_data;
					pqinfo->vec_id = cur_vecid;
					pqinfo->cmp_result = -1;
				}

				finish_key_cb(prdata);

				if (cur_data != SPT_INVALID)
					pclst->get_key_in_tree_end(pcur_data);
				return ret;
			case SPT_OP_INSERT:
				startbit += len;
				if (cmpres.smallfs == startbit
						&& startbit < endbit)
					st_insert_info.fs = find_fs(
							prdata, startbit,
							endbit-startbit);
				else
					st_insert_info.fs = cmpres.smallfs;
				st_insert_info.pkey_vec = pcur;
				st_insert_info.key_val = cur_vec.val;
				st_insert_info.cmp_pos = cmpres.pos;
				st_insert_info.signpost = signpost;
				st_insert_info.ref_cnt = pqinfo->multiple;
						st_insert_info.alloc_type = pqinfo->res;
						st_insert_info.key_id = cur_vecid;
				ret = do_insert_down_via_r(pclst,
						&st_insert_info, pdata);
				if (ret == SPT_DO_AGAIN)
					goto refind_start;

				if (ret >= 0) {
					pqinfo->db_id = ret;
					pqinfo->data = 0;
                            pqinfo->vec_id = st_insert_info.hang_vec;
					finish_key_cb(prdata);

					if (cur_data != SPT_INVALID)
						pclst->get_key_in_tree_end(
								pcur_data);
					return SPT_OK;
				}
				finish_key_cb(prdata);

				if (cur_data != SPT_INVALID)
					pclst->get_key_in_tree_end(
							pcur_data);
				return ret;

			case SPT_OP_DELETE:
				finish_key_cb(prdata);

				if (cur_data != SPT_INVALID)
					pclst->get_key_in_tree_end(pcur_data);
				return ret;
			default:
				break;
			}
		}
		continue;
		/*first bit is 0£¬start from pcur_vec->down*/
go_down:

		if (cur_data != SPT_INVALID) {
			/* verify the dbid, if changed refind from start*/
			if (cur_data != get_data_id(pclst, pcur))
				goto refind_start;
			cur_data = SPT_INVALID;
			pclst->get_key_in_tree_end(pcur_data);
		}
		while (fs_pos > startbit) {
			if (cur_vec.down != SPT_NULL)
				goto down_continue;
			if (direction == SPT_RIGHT) {
				tmp_vec.val = cur_vec.val;
				tmp_vec.status = SPT_VEC_INVALID;
				cur_vec.val = atomic64_cmpxchg(
						(atomic64_t *)pcur,
						cur_vec.val, tmp_vec.val);
				/*set invalid succ or not, refind from ppre*/
				pcur = ppre;
				goto refind_forward;
			}
			switch (op) {
			case SPT_OP_FIND:
				if (cur_data == SPT_INVALID) {
					cur_data = get_data_id(pclst, pcur);
					if (cur_data >= 0
						&& cur_data < SPT_INVALID) {
						//pqinfo->db_id = cur_data;
					} else if (cur_data == SPT_DO_AGAIN) {
						cur_data = SPT_INVALID;
						goto refind_start;
					} else {
						//SPT_NOMEM or SPT_WAIT_AMT;

						finish_key_cb(prdata);
						//if (ret == SPT_OK)
							//return ret;
						cur_data =
						(ret == SPT_OK) ? ret:cur_data;
						return cur_data;
					}
				}
				if (pqinfo->op == SPT_OP_FIND) {
					pqinfo->db_id = cur_data;
					pqinfo->vec_id = cur_vecid;
					pqinfo->cmp_result = -1;
				}

				finish_key_cb(prdata);
				return ret;
			case SPT_OP_INSERT:
				st_insert_info.pkey_vec = pcur;
				st_insert_info.key_val = cur_vec.val;
				st_insert_info.fs = fs_pos;
				st_insert_info.signpost = signpost;
				st_insert_info.key_id = cur_vecid;
				st_insert_info.ref_cnt = pqinfo->multiple;
							st_insert_info.alloc_type = pqinfo->res;
				ret = do_insert_last_down(pclst,
						&st_insert_info, pdata);
				if (ret == SPT_DO_AGAIN)
					goto refind_start;
				if (ret >= 0) {
					pqinfo->db_id = ret;
					pqinfo->data = 0;
                    pqinfo->vec_id = cur_vecid;
					ret = SPT_OK;
				}
				finish_key_cb(prdata);
				return ret;
			case SPT_OP_DELETE:

				finish_key_cb(prdata);
				return ret;
			default:
				break;
			}
down_continue:
			pnext = (struct spt_vec *)vec_id_2_ptr(pclst,
					cur_vec.down);
			next_vec.val = pnext->val;
			next_vecid = cur_vec.down;
			if (next_vec.status == SPT_VEC_RAW) {
				smp_mb();/* ^^^ */
				next_vec.val = pnext->val;
				if (next_vec.status == SPT_VEC_RAW)
					goto refind_start;
			}
			if (next_vec.status == SPT_VEC_INVALID) {
				tmp_vec.val = cur_vec.val;
				vecid = cur_vec.down;

				tmp_vec.down = next_vec.down;
				if (next_vec.type != SPT_VEC_DATA
					|| (next_vec.type == SPT_VEC_DATA
						&& next_vec.rd != SPT_NULL)) {
					struct spt_vec tmp_vec_b;

					tmp_vec_b.val = next_vec.val;
					tmp_vec_b.status = SPT_VEC_VALID;
					atomic64_cmpxchg((atomic64_t *)pnext,
							next_vec.val,
							tmp_vec_b.val);
			//set invalid succ or not, refind from cur
					cur_vec.val = pcur->val;
					if (cur_vec.status ==
							SPT_VEC_INVALID) {
						pcur = ppre;
						goto refind_forward;
					}
					continue;
				}
					//BUG();
				if (cur_vec.val == atomic64_cmpxchg(
							(atomic64_t *)pcur,
							cur_vec.val,
							tmp_vec.val)) {
					//delete_succ
					vec_free(pclst, vecid);
					retb = SPT_OK;
					if (retb != SPT_OK) {
						finish_key_cb(prdata);
						//if (ret == SPT_OK)
							//return ret;
						retb =
						(ret == SPT_OK) ? ret:retb;
						return retb;
					}
				}

				cur_vec.val = pcur->val;
				if (cur_vec.status != SPT_VEC_VALID) {
					pcur = ppre;
					goto refind_forward;
				}
				continue;

			}

			len = next_vec.pos + signpost - startbit + 1;

			direction = SPT_DOWN;
			/* signpost not used now*/

			#ifdef DEBUG_FIND_PATH
			if (op != SPT_OP_FIND) {
				int index;

				cur_data2 = get_data_id(pclst, pnext);
				if (cur_data2 >= 0 && cur_data2 < SPT_INVALID) {
					pdh2 = (struct spt_dh *)
						db_id_2_ptr(pclst, cur_data2);
					smp_mb();/* ^^^ */
					pcur_data2 =
						pclst->get_key_in_tree(
								pdh2->pdata);
					index = path_index[g_thrd_id]&0x3ff;
					debug_path[g_thrd_id][index].page
						= pdh2->pdata;
					debug_path[g_thrd_id][index].startbit
								= startbit;
					debug_path[g_thrd_id][index].len = len;
					debug_path[g_thrd_id][index].direct = 0;
					spt_bit_cpy(path_data[g_thrd_id],
							pcur_data2,
							startbit, len);
					pclst->get_key_in_tree_end(pcur_data2);
					path_index[g_thrd_id]++;
				} else {
					index = path_index[g_thrd_id]&0x3ff;
					spt_print("\r\n get dataid fail\r\n");
					debug_path[g_thrd_id][index].page = 0;
					debug_path[g_thrd_id][index].startbit
							= startbit;
					debug_path[g_thrd_id][index].len = len;
					debug_path[g_thrd_id][index].direct = 0;
					spt_bit_clear(path_data[g_thrd_id],
							startbit, len);
					path_index[g_thrd_id]++;
				}
			}
			#endif

			if (fs_pos >= startbit + len) {

				startbit += len;
				#if 0
				/*find the same record*/
				if (startbit >= endbit)
					break;
				#endif
				ppre = pcur;
				pcur = pnext;
				pre_vecid = cur_vecid;
				cur_vecid = next_vecid;
				cur_vec.val = next_vec.val;
				continue;
			}
			/*insert*/
			switch (op) {
			case SPT_OP_FIND:
				if (cur_data == SPT_INVALID) {
					cur_data = get_data_id(pclst, pcur);
					if (cur_data >= 0 &&
						cur_data < SPT_INVALID) {
						//pqinfo->db_id = cur_data;
						//debug
					} else if (cur_data == SPT_DO_AGAIN) {
						cur_data = SPT_INVALID;
						goto refind_start;

					} else {
						//SPT_NOMEM or SPT_WAIT_AMT;

						finish_key_cb(prdata);
						//if (ret == SPT_OK)
							//return ret;
						cur_data =
						(ret == SPT_OK) ? ret:cur_data;

						return cur_data;
					}
				}
				if (pqinfo->op == SPT_OP_FIND) {
					pqinfo->db_id = cur_data;
					pqinfo->vec_id = cur_vecid;
					pqinfo->cmp_result = -1;
				}

				finish_key_cb(prdata);
				return ret;
			case SPT_OP_INSERT:
				st_insert_info.pkey_vec = pcur;
				st_insert_info.key_val = cur_vec.val;
				st_insert_info.fs = fs_pos;
				st_insert_info.signpost = signpost;
				st_insert_info.key_id = cur_vecid;
				st_insert_info.ref_cnt = pqinfo->multiple;
				st_insert_info.alloc_type = pqinfo->res;
				ret = do_insert_up_via_d(pclst,
						&st_insert_info, pdata);
				if (ret == SPT_DO_AGAIN)
					goto refind_start;
				if (ret >= 0) {
					pqinfo->db_id = ret;
					pqinfo->data = 0;
                    pqinfo->vec_id = cur_vecid;
					ret = SPT_OK;
				}
				finish_key_cb(prdata);
				return ret;
			case SPT_OP_DELETE:

				finish_key_cb(prdata);
				return ret;
			default:
				break;
			}

		}
		spt_assert(fs_pos == startbit);

	}

	ret = SPT_OK;

	if (cur_data == SPT_INVALID) {
		if (cur_vec.type != SPT_VEC_DATA)
			spt_assert(0);
		cur_data = cur_vec.rd;

		pdh = (struct spt_dh *)db_id_2_ptr(pclst, cur_data);
		smp_mb();/* ^^^ */
		//pcur_data = pclst->get_key(pdh->pdata);
	} else
		pclst->get_key_in_tree_end(pcur_data);

	switch (op) {
	case SPT_OP_FIND:
		if (pqinfo->op == SPT_OP_FIND) {
			pqinfo->db_id = cur_data;
			pqinfo->vec_id = cur_vecid;
			pqinfo->cmp_result = 0;
		}

		finish_key_cb(prdata);
		return ret;
	case SPT_OP_INSERT:
		while (1) {
			va_old = pdh->ref;
			if (va_old == 0) {
				cur_data = SPT_INVALID;
				ret = SPT_NOT_FOUND;
				goto refind_start;
			} else if (va_old > 0) {
				va_new = va_old + pqinfo->multiple;
				if (va_old == atomic_cmpxchg(
							(atomic_t *)&pdh->ref,
							va_old, va_new))
					break;
			} else
				spt_assert(0);
		}
		pqinfo->db_id = cur_data;
		pqinfo->ref_cnt = va_new;
        pqinfo->vec_id = cur_vecid;
		finish_key_cb(prdata);
		return ret;
	case SPT_OP_DELETE:
		pqinfo->db_id = cur_data;
		while (1) {
			va_old = pdh->ref;
			if (va_old == 0) {

				finish_key_cb(prdata);
				return SPT_NOT_FOUND;
			} else if (va_old > 0) {
				if (va_old < pqinfo->multiple) {
					spt_assert(0);
					return SPT_NOT_FOUND;
				}
				va_new = va_old-pqinfo->multiple;
				if (va_old == atomic_cmpxchg(
							(atomic_t *)&pdh->ref,
							va_old, va_new))
					break;
			} else
				spt_assert(0);
		}
		//pqinfo->ref_cnt = va_new;
		if (va_new == 0) {
			tmp_vec.val = cur_vec.val;
			tmp_vec.rd = SPT_NULL;
			if (pcur == (struct spt_vec *)vec_id_2_ptr(pclst,
						pclst->vec_head)) {
				if (cur_vec.val == atomic64_cmpxchg(
							(atomic64_t *)pcur,
							cur_vec.val,
							tmp_vec.val)) {
					//invalidate succ
					spt_set_data_free_flag(pdh,
							pqinfo->free_flag);
                    db_free(pclst,cur_data);
                    
                    finish_key_cb(prdata);
                    pqinfo->ref_cnt = 0;
                    return SPT_OK;
                }
                else
                {
                    cur_data = SPT_INVALID;
                    //atomic_add_return(1,(atomic_t *)&pdh->ref);
                    while(1)
                    {
                        va_old = pdh->ref;
                        va_new = va_old+pqinfo->multiple;
                        if(va_old == atomic_cmpxchg((atomic_t *)&pdh->ref, va_old,va_new))
                            break;
                    }
                    
                    ret = SPT_NOT_FOUND;
                    goto refind_start;
                }
            }
            tmp_vec.status = SPT_VEC_INVALID;
            if(cur_vec.val == atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val))//invalidate succ
            {
                spt_set_data_free_flag(pdh, pqinfo->free_flag);
                db_free(pclst,cur_data);
                if(!pclst->is_bottom)
                    refresh_db_hang_vec(pclst, pcur, pdh);
                op = SPT_OP_FIND;
                //pcur = ppre;
                cur_data = SPT_INVALID;
                pqinfo->ref_cnt = 0;
                //goto refind_forward;
                goto refind_start;
            }
            else
            {
                //atomic_add_return(1,(atomic_t *)&pdh->ref);
                while(1)
                {
                    va_old = pdh->ref;
                    va_new = va_old+pqinfo->multiple;
                    if(va_old == atomic_cmpxchg((atomic_t *)&pdh->ref, va_old,va_new))
                        break;
                }                
                cur_data = SPT_INVALID;
                ret = SPT_NOT_FOUND;
                goto refind_start;
            }
        }
        
        finish_key_cb(prdata);
        pqinfo->ref_cnt = va_new;
        return SPT_OK;
    default:
        break;
    }
    
    finish_key_cb(prdata);
    return SPT_ERR;
}
/*
 * Insert a data into a cluster
 */
int do_insert_data(struct cluster_head_t *pclst,
				char *pdata,
				spt_cb_get_key pf,
				spt_cb_end_key pf2)
{
	struct query_info_t qinfo = {0};
	struct spt_vec *pvec_start;

	pvec_start = (struct spt_vec *)vec_id_2_ptr(pclst, pclst->vec_head);

	qinfo.op = SPT_OP_INSERT;
	qinfo.signpost = 0;
	qinfo.pstart_vec = pvec_start;
	qinfo.startid = pclst->vec_head;
	qinfo.endbit = pclst->endbit;
	qinfo.data = pdata;
	qinfo.multiple = 1;
	qinfo.get_key = pf;
	qinfo.get_key_end = pf2;
	qinfo.res = SPT_TOP_INSERT;

	return find_data(pclst, &qinfo);
}
/*
 * Insert multiple copies of a data one time into a cluster
 */
int do_insert_data_multiple(struct cluster_head_t *pclst,
				char *pdata,
				int cnt,
				spt_cb_get_key pf,
				spt_cb_end_key pf2)
{
	struct query_info_t qinfo = {0};
	struct spt_vec *pvec_start;

	pvec_start = (struct spt_vec *)vec_id_2_ptr(pclst, pclst->vec_head);

	qinfo.op = SPT_OP_INSERT;
	qinfo.signpost = 0;
	qinfo.pstart_vec = pvec_start;
	qinfo.startid = pclst->vec_head;
	qinfo.endbit = pclst->endbit;
	qinfo.data = pdata;
	qinfo.multiple = cnt;
	qinfo.get_key = pf;
	qinfo.get_key_end = pf2;

	return find_data(pclst, &qinfo);
}

/*
 * Delete a data from a cluster
 */
int do_delete_data(struct cluster_head_t *pclst,
				char *pdata,
				spt_cb_get_key pf,
				spt_cb_end_key pf2)
{
	struct query_info_t qinfo = {0};
	struct spt_vec *pvec_start;

	pvec_start = (struct spt_vec *)vec_id_2_ptr(pclst, pclst->vec_head);

	qinfo.op = SPT_OP_DELETE;
	qinfo.signpost = 0;
	qinfo.pstart_vec = pvec_start;
	qinfo.startid = pclst->vec_head;
	qinfo.endbit = pclst->endbit;
	qinfo.data = pdata;
	qinfo.multiple = 1;
	qinfo.free_flag = 1;
	qinfo.get_key = pf;
	qinfo.get_key_end = pf2;

	return find_data(pclst, &qinfo);
}
/*
 * delete multiple copies of a data one time from a cluster,
 * if the ref_cnt is 0 after delete ,cluster need not free the data.
 */
int do_delete_data_no_free_multiple(struct cluster_head_t *pclst,
						char *pdata,
						int cnt,
						spt_cb_get_key pf,
						spt_cb_end_key pf2)
{
	struct query_info_t qinfo = {0};
	struct spt_vec *pvec_start;

	pvec_start = (struct spt_vec *)vec_id_2_ptr(pclst, pclst->vec_head);

	qinfo.op = SPT_OP_DELETE;
	qinfo.signpost = 0;
	qinfo.pstart_vec = pvec_start;
	qinfo.startid = pclst->vec_head;
	qinfo.endbit = DATA_BIT_MAX;
	qinfo.data = pdata;
	qinfo.multiple = cnt;
	qinfo.free_flag = 0;
	qinfo.get_key = pf;
	qinfo.get_key_end = pf2;

	return find_data(pclst, &qinfo);
}
/**
 * find_next_cluster - find the next level cluster in the up-level cluster
 * @pclst: pointer of sd tree cluster head
 * @pdata: pointer of data to insert
 *
 * a and b are two adjacent data in the cluster.
 * if a < *pdata <= b;
 * return b->cluster.
 */
struct cluster_head_t *find_next_cluster(struct cluster_head_t *pclst,
		char *pdata)
{
	struct query_info_t qinfo;
	int ret;
	struct spt_dh *pdh;
	struct spt_dh_ext *pdext_h;
	struct spt_vec *phang_vec, *pvec, vec;

refind_next:
	memset(&qinfo, 0, sizeof(struct query_info_t));
	qinfo.op = SPT_OP_FIND;
	qinfo.signpost = 0;
	qinfo.pstart_vec = pclst->pstart;
	qinfo.startid = pclst->vec_head;
	qinfo.endbit = pclst->endbit;
	qinfo.data = pdata;
//	qinfo.get_key = pf;

	ret = find_data(pclst, &qinfo);
	if (ret >= 0) {
		pdh = (struct spt_dh *)db_id_2_ptr(pclst, qinfo.db_id);
		pdext_h = (struct spt_dh_ext *)pdh->pdata;
		if (qinfo.cmp_result == 0)
			return pdext_h->plower_clst;
		else if (qinfo.cmp_result < 0) {
			/* < 0 continue to look down */
			pvec = (struct spt_vec *)vec_id_2_ptr(pclst,
					qinfo.vec_id);
			vec.val = pvec->val;
			/* difference appear in leaf node*/
			if (vec.type == SPT_VEC_DATA)
				return pdext_h->plower_clst;

			pvec = (struct spt_vec *)vec_id_2_ptr(pclst, vec.rd);
			ret = find_lowest_data(pclst, pvec);
			if (ret == SPT_NULL)
				goto refind_next;
			pdh = (struct spt_dh *)db_id_2_ptr(pclst, ret);
			pdext_h = (struct spt_dh_ext *)pdh->pdata;
			return pdext_h->plower_clst;

		} else {
			/* > 0 continue to look from hang vec*/
			if (pdext_h->hang_vec >= SPT_INVALID)
				spt_debug("hang_vec id too big :%d\r\n",
						pdext_h->hang_vec);
			phang_vec = (struct spt_vec *)vec_id_2_ptr(pclst,
					pdext_h->hang_vec);
			vec.val = phang_vec->val;
			if (vec.type == SPT_VEC_DATA) {
				if (vec.rd == SPT_NULL)
					goto refind_next;
				pdh = (struct spt_dh *)db_id_2_ptr(pclst,
						vec.rd);
				pdext_h = (struct spt_dh_ext *)pdh->pdata;
				return pdext_h->plower_clst;
			}
			pvec = (struct spt_vec *)vec_id_2_ptr(pclst, vec.rd);
			ret = find_lowest_data(pclst, pvec);
			if (ret == SPT_NULL)
				goto refind_next;
			pdh = (struct spt_dh *)db_id_2_ptr(pclst, ret);
			pdext_h = (struct spt_dh_ext *)pdh->pdata;
			return pdext_h->plower_clst;

		}
	} else {
		spt_debug("find_data err!\r\n");
		spt_assert(0);
		return NULL;
	}
}
/**
 * insert_data - insert data into sd tree
 * @pclst: pointer of sd tree cluster head
 * @pdata: pointer of data to insert
 *
 * Insert data into sd tree and return a pointer
 * if there is no duplicate in the tree, the pointer point to pdata
 * if there is duplicate in the tree, the pointer point to the copy
 * sd-tree free the original pdata
 */
char *insert_data(struct cluster_head_t *pclst, char *pdata)
{
	struct cluster_head_t *pnext_clst;
	struct query_info_t qinfo = {0};
	struct spt_dh *pdh;
	int ret = 0;

	/*
	 *first look up in the top cluster.
	 *which next level cluster do the data belong.
	 */
	pnext_clst = find_next_cluster(pclst, pdata);
	if (pnext_clst == NULL) {
		spt_set_errno(SPT_MASKED);
		return 0;
	}
	if (pnext_clst->data_total >= SPT_DATA_HIGH_WATER_MARK
		|| pnext_clst->ins_mask == 1) {
		spt_set_errno(SPT_MASKED);
		return 0;
	}
	qinfo.op = SPT_OP_INSERT;
	qinfo.signpost = 0;
	qinfo.pstart_vec = pnext_clst->pstart;
	qinfo.startid = pnext_clst->vec_head;
	qinfo.endbit = pnext_clst->endbit;
	qinfo.data = pdata;
	qinfo.multiple = 1;

	/*
	 *insert data into the final cluster
	 */
	ret = find_data(pnext_clst, &qinfo);
	if (ret >= 0) {
		pdh = (struct spt_dh *)db_id_2_ptr(pnext_clst, qinfo.db_id);
		return pdh->pdata;
	}
	spt_set_errno(ret);
	return 0;

}
/**
 * delete_data - delete data from sd tree
 * @pclst: pointer of sd tree cluster head
 * @pdata: pointer of data to delete
 *
 * Delete data into sd tree and
 * return the duplicate count in sd tree if delete successful
 * if there is no data match in the tree ,return value < 0
 */
char *delete_data(struct cluster_head_t *pclst, char *pdata)
{
	struct cluster_head_t *pnext_clst;
	struct query_info_t qinfo = {0};
	struct spt_dh *pdh;
	int ret = 0;
	/*
	 *first look up in the top cluster.
	 *which next level cluster do the data belong.
	 */
	pnext_clst = find_next_cluster(pclst, pdata);
	if (pnext_clst == NULL) {
		spt_set_errno(SPT_MASKED);
		return NULL;
	}

	qinfo.op = SPT_OP_DELETE;
	qinfo.signpost = 0;
	qinfo.pstart_vec = pnext_clst->pstart;
	qinfo.startid = pnext_clst->vec_head;
	qinfo.endbit = pnext_clst->endbit;
	qinfo.data = pdata;
	qinfo.multiple = 1;
	qinfo.ref_cnt = 0;
	qinfo.free_flag = 0;
	qinfo.res = SPT_USER_INSERT;
	/*
	 *insert data into the final cluster
	 */
	ret = find_data(pnext_clst, &qinfo);
	if (ret == 0) { /*delete ok*/
		pdh = (struct spt_dh *)db_id_2_ptr(pnext_clst,
				qinfo.db_id);
		if (!pdh->pdata)
			spt_assert(0);
		return pdh->pdata;
	}
	spt_set_errno(ret);
	return NULL;
}
char *query_data(struct cluster_head_t *pclst, char *pdata)
{
	struct cluster_head_t *pnext_clst;
	struct query_info_t qinfo = {0};
	struct spt_dh *pdh;
	int ret = 0;
	/*
	 *first look up in the top cluster.
	 *which next level cluster do the data belong.
	 */
	pnext_clst = find_next_cluster(pclst, pdata);
	if (pnext_clst == NULL) {
		spt_set_errno(SPT_MASKED);
		return NULL;
	}

	qinfo.op = SPT_OP_FIND;
	qinfo.signpost = 0;
	qinfo.pstart_vec = pnext_clst->pstart;
	qinfo.startid = pnext_clst->vec_head;
	qinfo.endbit = pnext_clst->endbit;
	qinfo.data = pdata;
	/*
	 *find data into the final cluster
	 */
	ret = find_data(pnext_clst, &qinfo);
	if (ret == 0) { /*delete ok*/
		pdh = (struct spt_dh *)db_id_2_ptr(pnext_clst,
				qinfo.db_id);
		if (!pdh->pdata)
			spt_assert(0);
		return pdh->pdata;
	}
	spt_set_errno(ret);
	return NULL;
}
int query_data_prediction(struct cluster_head_t *pclst, char *pdata)
{
	struct cluster_head_t *pnext_clst;
	struct prediction_info_t pre_qinfo = {0};
	struct query_info_t qinfo = {0};
	struct spt_dh *pdh;
	int ret = 0;
	/*
	 *first look up in the top cluster.
	 *which next level cluster do the data belong.
	 */
	pnext_clst = find_next_cluster(pclst, pdata);
	if (pnext_clst == NULL) {
		spt_set_errno(SPT_MASKED);
		return NULL;
	}

	pre_qinfo.pstart_vec = pnext_clst->pstart;
	pre_qinfo.startid = pnext_clst->vec_head;
	pre_qinfo.endbit = pnext_clst->endbit;
	pre_qinfo.data = pdata;
	ret = find_data_prediction(pnext_clst, &pre_qinfo);
	if (ret == 0) { /*delete ok*/
		qinfo.op = SPT_OP_FIND;
		qinfo.signpost = 0;
		qinfo.pstart_vec = pre_qinfo.ret_vec;
		qinfo.startid = pre_qinfo.ret_vec_id;
		qinfo.endbit = pnext_clst->endbit;
		qinfo.data = pdata;
		/*
		 *find data into the final cluster
		 */
		ret = find_data(pnext_clst, &qinfo);
		if (ret == 0) { /*delete ok*/
			pdh = (struct spt_dh *)db_id_2_ptr(pnext_clst,
					qinfo.db_id);
			if (!pdh->pdata)
				spt_assert(0);
			return pdh->pdata;
		}
		printf("find data prediction err\r\n");
	}
	printf("prediction err\r\n");
	spt_set_errno(ret);
	return -1;
}
char *insert_data_prediction(struct cluster_head_t *pclst, char *pdata)
{
	struct cluster_head_t *pnext_clst;
	struct query_info_t qinfo = {0};
	struct prediction_info_t pre_qinfo = {0};
	struct spt_dh *pdh;
	int ret = 0;

	/*
	 *first look up in the top cluster.
	 *which next level cluster do the data belong.
	 */
	pnext_clst = find_next_cluster(pclst, pdata);
	if (pnext_clst == NULL) {
		spt_set_errno(SPT_MASKED);
		return 0;
	}
	if (pnext_clst->data_total >= SPT_DATA_HIGH_WATER_MARK
		|| pnext_clst->ins_mask == 1) {
		spt_set_errno(SPT_MASKED);
		return 0;
	}
	
	pre_qinfo.pstart_vec = pnext_clst->pstart;
	pre_qinfo.startid = pnext_clst->vec_head;
	pre_qinfo.endbit = pnext_clst->endbit;
	pre_qinfo.data = pdata;

	ret = find_data_prediction(pnext_clst, &pre_qinfo);

	qinfo.op = SPT_OP_INSERT;
	qinfo.signpost = 0;
	qinfo.data = pdata;
	qinfo.multiple = 1;
	qinfo.endbit = pnext_clst->endbit;
	
	if (ret == 0) {
		qinfo.pstart_vec = pre_qinfo.ret_vec;
		qinfo.startid = pre_qinfo.ret_vec_id;
		
		ret = find_data(pnext_clst, &qinfo);
		if (ret >= 0) {
			pdh = (struct spt_dh *)db_id_2_ptr(pnext_clst, qinfo.db_id);
			return pdh->pdata;
		}
	}

	qinfo.pstart_vec = pnext_clst->pstart;
	qinfo.startid = pnext_clst->vec_head;
	/*
	 *insert data into the final cluster
	 */
	ret = find_data(pnext_clst, &qinfo);
	if (ret >= 0) {
		pdh = (struct spt_dh *)db_id_2_ptr(pnext_clst, qinfo.db_id);
		return pdh->pdata;
	}
	spt_set_errno(ret);
	return NULL;
}


char *delete_data_prediction(struct cluster_head_t *pclst, char *pdata)
{
	struct cluster_head_t *pnext_clst;
	struct query_info_t qinfo = {0};
	struct prediction_info_t pre_qinfo = {0};
	struct spt_dh *pdh;
	int ret = 0;
	/*
	 *first look up in the top cluster.
	 *which next level cluster do the data belong.
	 */
	pnext_clst = find_next_cluster(pclst, pdata);
	if (pnext_clst == NULL) {
		spt_set_errno(SPT_MASKED);
		return NULL;
	}
	pre_qinfo.pstart_vec = pnext_clst->pstart;
	pre_qinfo.startid = pnext_clst->vec_head;
	pre_qinfo.endbit = pnext_clst->endbit;
	pre_qinfo.data = pdata;

	ret = find_data_prediction(pnext_clst, &pre_qinfo);
	
	qinfo.op = SPT_OP_DELETE;
	qinfo.signpost = 0;
	qinfo.data = pdata;
	qinfo.multiple = 1;
	qinfo.ref_cnt = 0;
	qinfo.free_flag = 0;
	qinfo.endbit = pnext_clst->endbit;
	if (ret == 0) {
		qinfo.pstart_vec = pre_qinfo.ret_vec;
		qinfo.startid = pre_qinfo.ret_vec_id;
		
		ret = find_data(pnext_clst, &qinfo);
		if (ret == 0) {
			pdh = (struct spt_dh *)db_id_2_ptr(pnext_clst, qinfo.db_id);
			if (!pdh->pdata)
				spt_assert(0);
			return pdh->pdata;
		}
	}

	qinfo.pstart_vec = pnext_clst->pstart;
	qinfo.startid = pnext_clst->vec_head;
	
	ret = find_data(pnext_clst, &qinfo);
	if (ret == 0) { /*delete ok*/
		pdh = (struct spt_dh *)db_id_2_ptr(pnext_clst,
				qinfo.db_id);
		if (!pdh->pdata)
			spt_assert(0);
		return pdh->pdata;
	}
	spt_set_errno(ret);
	return NULL;
}

/**
 * spt_cluster_init - init sd tree
 * @startbit: the effective bit of data start in the tree
 * @endbit: the effective bit of data end in the tree
 * @thread_num: how many thread will  parallel process
 * @pf: user callback function, the tree call it get actual user data
 * @pf2: user callback function, the tree call it end accessing user data
 * @pf_free: user callback function, the tree call it to free user data
 * @pf_con: divided cluster will user it
 * Init sd tree, the tree is multi level structure, data is saved in the bottom
 * cluster.
 * return the pointer of the sd tree cluster head
 */
struct cluster_head_t *spt_cluster_init(u64 startbit,
							u64 endbit,
							int thread_num,
							spt_cb_get_key pf,
							spt_cb_end_key pf2,
							spt_cb_free pf_free,
							spt_cb_construct pf_con)
{
	struct cluster_head_t *pclst, *plower_clst;
	struct spt_dh_ext *pdh_ext;
	char *pdata;
	int i;
	/*
	 * init top cluster
	 */
	pclst = cluster_init(0, startbit, DATA_BIT_MAX,
			thread_num, pf, pf2, free_data,
				spt_upper_construct_data);

	if (pclst == NULL)
		return NULL;
	INIT_LIST_HEAD(&pclst->c_list);
	/*
	 * init bottom cluster
	 */
	plower_clst = cluster_init(1, startbit,
			endbit, thread_num, pf, pf2,
					pf_free, pf_con);

	if (plower_clst == NULL) {
		cluster_destroy(pclst);
		return NULL;
	}

	pdh_ext = spt_malloc(sizeof(struct spt_dh_ext)+DATA_SIZE);
	if (pdh_ext == NULL) {
		cluster_destroy(pclst);
		cluster_destroy(plower_clst);
		return NULL;
	}
	pdh_ext->data = (char *)(pdh_ext+1);
	pdh_ext->plower_clst = plower_clst;
	memset(pdh_ext->data, 0xff, DATA_SIZE);

	do_insert_data(pclst, (char *)pdh_ext,
			pclst->get_key_in_tree,
			pclst->get_key_in_tree_end);
	list_add(&plower_clst->c_list, &pclst->c_list);


	plower_clst = cluster_init(1, startbit, endbit,
			thread_num, pf, pf2,
			pf_free, pf_con);

	if (plower_clst == NULL) {
		cluster_destroy(pclst);
		return NULL;
	}

	pdh_ext = spt_malloc(sizeof(struct spt_dh_ext)+DATA_SIZE);
	if (pdh_ext == NULL) {
		cluster_destroy(pclst);
		cluster_destroy(plower_clst);
		return NULL;
	}
	pdh_ext->data = (char *)(pdh_ext+1);
	pdh_ext->plower_clst = plower_clst;
	pdata = pdh_ext->data + DATA_SIZE - 2;
	*pdata = 32;

	do_insert_data(pclst, (char *)pdh_ext,
			pclst->get_key_in_tree,
			pclst->get_key_in_tree_end);
	list_add(&plower_clst->c_list, &pclst->c_list);

	/*
	 * The sample space is divided into several parts on average
	 */
	for (i = 1; i < 256; i++) {
		plower_clst = cluster_init(1, startbit,
				endbit, thread_num, pf, pf2,
							pf_free, pf_con);
		if (plower_clst == NULL) {
			cluster_destroy(pclst);
			return NULL;
		}

		pdh_ext = spt_malloc(sizeof(struct spt_dh_ext));
		if (pdh_ext == NULL) {
			cluster_destroy(pclst);
			cluster_destroy(plower_clst);
			return NULL;
		}
		pdh_ext->data = spt_malloc(DATA_SIZE);
		if (pdh_ext->data == NULL) {
			cluster_destroy(pclst);
			return NULL;
		}
		memset(pdh_ext->data, 0, DATA_SIZE);
		*pdh_ext->data = i;
//		get_random_bytes(pdh_ext->data,DATA_SIZE);
		pdh_ext->plower_clst = plower_clst;
		do_insert_data(pclst, (char *)pdh_ext,
				pclst->get_key_in_tree,
				pclst->get_key_in_tree_end);
		list_add(&plower_clst->c_list, &pclst->c_list);
	}
	//debug_cluster_travl(pclst);
	return pclst;
}

#ifdef _BIG_ENDIAN
/*	must be byte aligned
 *	len:bit len,
 */
void find_smallfs(u8 *a, s64 len, int align, struct vec_cmpret_t *result)
{
	u8 uca;
	u64 bitend, ulla, fs;
	s64 lenbyte;
	u32 uia;
	u16 usa;

	bitend = len%8;
	lenbyte = len/8;

	switch (align) {
	case 8:
		while (lenbyte >= 8) {
			ulla = *(u64 *)a; a += 8;
			if (ulla != 0) {
				fs = ullfind_firt_set(ulla);
				result->smallfs += fs;
				result->finish = 1;
				return;
			}
			result->smallfs += 64;
			lenbyte -= 8;
		}
	case 4:
		while (lenbyte >= 4) {
			uia = *(u32 *)a; a += 4;
			if (uia != 0) {
				fs = uifind_firt_set(uia);
				result->smallfs += fs;
				result->finish = 1;
				return;
			}
			result->smallfs += 32;
			lenbyte -= 4;
		}
	case 2:
		while (lenbyte >= 2) {
			usa = *(u16 *)a; a += 2;
			if (usa != 0) {
				fs = usfind_firt_set(usa);
				result->smallfs += fs;
				result->finish = 1;
				return;
			}
			result->smallfs += 16;
			lenbyte -= 2;
		}
	case 1:
		while (lenbyte >= 1) {
			uca = *a; a++;
			if (uca != 0) {
				fs = ucfind_firt_set(uca);
				result->smallfs += fs;
				result->finish = 1;
				return;
			}
			result->smallfs += 8;
			lenbyte--;
		}
		break;
	default:
		spt_print("\n%s\t%d", __func__, __LINE__);
		spt_assert(0);

	}

	if (bitend) {
		uca = *a;
		fs = ucfind_firt_set(uca);
		if (fs >= bitend)
			result->smallfs += bitend;
		else
			result->smallfs += fs;
	}
//	result->finish = 1;
}

int align_compare(u8 *a, u8 *b, s64 len, int align, struct vec_cmpret_t *result)
{
	u8 uca, ucb, ucres, bitend;
	u64 ulla, ullb, ullres, fs;
	s64 lenbyte;
	u32 uia, uib, uires;
	u16 usa, usb, usres;
	int ret = 0;
	u8 *c;

	bitend = len%8;
	lenbyte = len/8;

	switch (align) {
	case 8:
		while (lenbyte >= 8) {
			ulla = *(u64 *)a; a += 8;
			ullb = *(u64 *)b; b += 8;

			if (ulla == ullb) {
				result->pos += 64;
				lenbyte -= 8;
				continue;
			} else {
				ullres = ulla^ullb;
				fs = ullfind_firt_set(ullres);
				result->smallfs = result->pos;
				result->pos += fs;
			//	result->flag = 0;
				if (ulla > ullb) {
					ret = 1;
					c = b;
					ulla = ullb;
				} else {
					ret = -1;
					c = a;
				}
				ulla = ulla << fs;
				if (ulla != 0) {
					fs = ullfind_firt_set(ulla);
					result->smallfs = result->pos + fs;
					result->finish = 1;
				} else {
					result->smallfs += 64;
					find_smallfs(c,
							(lenbyte<<3)+bitend,
							8, result);
					//result->finish = 1;
				}
				return ret;
			}
		}
	case 4:
		while (lenbyte >= 4) {
			uia = *(u32 *)a; a += 4;
			uib = *(u32 *)b; b += 4;

			if (uia == uib) {
				result->pos += 32;
				lenbyte -= 4;
				continue;
			} else {
				uires = uia^uib;
				fs = uifind_firt_set(uires);
				result->smallfs = result->pos;
				result->pos += fs;

				if (uia > uib) {
					ret = 1;
					c = b;
					uia = uib;
				} else {
					ret = -1;
					c = a;
				}
				uia = uia << fs;
				if (uia != 0) {
					fs = uifind_firt_set(uia);
					result->smallfs = result->pos + fs;
					result->finish = 1;
				} else {
					result->smallfs += 32;
					find_smallfs(c,
							(lenbyte<<3)+bitend,
							4, result);
					//result->finish = 1;
				}
				return ret;
			}
		}

	case 2:
		while (lenbyte >= 2) {
			usa = *(u16 *)a; a += 2;
			usb = *(u16 *)b; b += 2;

			if (usa == usb) {
				result->pos += 16;
				lenbyte -= 2;
				continue;
			} else {
				usres = usa^usb;
				fs = usfind_firt_set(usres);
				result->smallfs = result->pos;
				result->pos += fs;

				if (usa > usb) {
					ret = 1;
					c = b;
					usa = usb;
				} else {
					ret = -1;
					c = a;
				}
				usa = usa << fs;
				if (usa != 0) {
					fs = usfind_firt_set(usa);
					result->smallfs = result->pos + fs;
					result->finish = 1;
				} else {
					result->smallfs += 16;
					find_smallfs(c,
							(lenbyte<<3)+bitend,
							2, result);
					//result->finish = 1;
				}
				return ret;
			}
		}
	case 1:
		while (lenbyte >= 1) {
			uca = *(u64 *)a; a++;
			ucb = *(u64 *)b; b++;

			if (uca == ucb) {
				result->pos += 8;
				lenbyte--;
				continue;
			} else {
				ucres = uca^ucb;
				fs = ucfind_firt_set(ucres);
				result->smallfs = result->pos;
				result->pos += fs;

				if (uca > ucb) {
					ret = 1;
					c = b;
					uca = ucb;
				} else {
					ret = -1;
					c = a;
				}
				uca = uca << fs;
				if (uca != 0) {
					fs = ucfind_firt_set(uca);
					result->smallfs = result->pos + fs;
					result->finish = 1;
				} else {
					result->smallfs += 8;
					find_smallfs(c,
							(lenbyte<<3)+bitend,
							1, result);
					//result->finish = 1;
				}
				return ret;
			}
		}
		break;
	default:
		spt_print("\n%s\t%d", __func__, __LINE__);
		spt_assert(0);
	}

	if (bitend) {
		uca = *a;
		ucb = *b;
		uca = uca >> (8-bitend) << (8-bitend);
		ucb = ucb >> (8-bitend) << (8-bitend);
		if (uca == ucb) {
			result->pos += bitend;
			result->smallfs = result->pos;
			ret = 0;
		} else {
			ucres = uca^ucb;
			fs = ucfind_firt_set(ucres);
			result->smallfs = result->pos;
			result->pos += fs;

			if (uca > ucb) {
				ret = 1;
				c = b;
				uca = ucb;
			} else {
				ret = -1;
				c = a;
			}
			uca = uca << fs;
			if (uca != 0) {
				fs = ucfind_firt_set(uca);
				result->smallfs = result->pos + fs;
				result->finish = 1;
			} else {
				result->smallfs += bitend;
			//	result->finish = 1;
			}

		}
	}

	return ret;

}

u64 find_fs(char *a, u64 start, u64 len)
{
	s64 align;
	u64 fs, ret, ulla;
	s64 lenbyte;
	u32 uia;
	u16 usa;
	u8 uca, bitstart, bitend;
	u8 *acstart;
	int ret = 0;

	fs = 0;
	bitstart = start%8;
	bitend = (bitstart + len)%8;
	acstart = (u8 *)a + start/8;
	lenbyte =  (bitstart + len)/8;
	ret = start;

	if (lenbyte == 0) {
		uca = *acstart;
		uca &= (1<<(8-bitstart))-1;
		fs = ucfind_firt_set(uca);
		if (fs >= bitend)
			return start + len;
		else
			return start + fs - bitstart;
	}

	if (bitstart != 0) {
		lenbyte--;
		uca = *acstart; acstart++;
		uca &= (1<<(8-bitstart))-1;
		if (uca == 0)
			ret += 8 - bitstart;
		else {
			fs = ucfind_firt_set(uca);
			ret += fs-bitstart;
			return ret;
		}
	}

	if (((unsigned long)acstart%8) != 0) {
		align = (unsigned long)acstart%8;
		align = 8-align;
		if (lenbyte < align) {
			while (lenbyte >= 1) {
				uca = *acstart; acstart++;
				if (uca != 0) {
					fs = ucfind_firt_set(uca);
					ret += fs;
					return ret;
				}
				ret += 8;
				lenbyte--;
			}
		} else {
			while (align >= 1) {
				uca = *acstart; acstart++;
				if (uca != 0) {
					fs = ucfind_firt_set(uca);
					ret += fs;
					return ret;
				}
				ret += 8;
				align--;
			}
			lenbyte -= align;
		}

	}

	while (lenbyte >= 8) {
		ulla = *(u64 *)acstart; acstart += 8;
		if (ulla != 0) {
			fs = ullfind_firt_set(ulla);
			ret += fs;
			return ret;
		}
		ret += 64;
		lenbyte -= 8;
	}
	while (lenbyte >= 4) {
		uia = *(u32 *)acstart; acstart += 4;
		if (uia != 0) {
			fs = uifind_firt_set(uia);
			ret += fs;
			return ret;
		}
		ret += 32;
		lenbyte -= 4;
	}
	while (lenbyte >= 2) {
		usa = *(u16 *)acstart; acstart += 2;
		if (usa != 0) {
			fs = usfind_firt_set(usa);
			ret += fs;
			return ret;
		}
		ret += 16;
		lenbyte -= 2;
	}
	while (lenbyte >= 1) {
		uca = *acstart; acstart++;
		if (uca != 0) {
			fs = ucfind_firt_set(uca);
			ret += fs;
			return ret;
		}
		ret += 8;
		lenbyte--;
	}

	if (bitend) {
		uca = *acstart;
		fs = ucfind_firt_set(uca);
		if (fs >= bitend)
			ret += bitend;
		else
			ret += fs;
	}
	return ret;
}

#else
void find_smallfs(u8 *a, s64 len, int align, struct vec_cmpret_t *result)
{
	u8 uca;
	u64 bitend, ulla, fs;
	s64 lenbyte;
	u32 uia;
	u16 usa;
	int i;

	bitend = len%8;
	lenbyte = len/8;

	switch (align) {
	case 8:
		while (lenbyte >= 8) {
			ulla = *(u64 *)a;
			if (ulla == 0) {
				result->smallfs += 64;
				lenbyte -= 8;
				a += 8;
				continue;
			}
			for (i = 0; i < 8; i++) {
				uca = *a;
				if (uca != 0) {
					fs = ucfind_firt_set(uca);
					result->smallfs += fs;
					result->finish = 1;
					return;
				}
				result->smallfs += 8;
				a++;
			}
			result->smallfs += 64;
			lenbyte -= 8;
			a += 8;
		}
	case 4:
		while (lenbyte >= 4) {
			uia = *(u32 *)a;
			if (uia == 0) {
				result->smallfs += 32;
				lenbyte -= 4;
				a += 4;
				continue;
			}
			for (i = 0; i < 4; i++) {
				uca = *a;
				if (uca != 0) {
					fs = ucfind_firt_set(uca);
					result->smallfs += fs;
					result->finish = 1;
					return;
				}
				result->smallfs += 8;
				a++;
			}
			result->smallfs += 32;
			lenbyte -= 4;
			a += 4;
		}
	case 2:
		while (lenbyte >= 2) {
			usa = *(u16 *)a;
			if (usa == 0) {
				result->smallfs += 16;
				lenbyte -= 2;
				a += 2;
				continue;
			}
			for (i = 0; i < 2; i++) {
				uca = *a;
				if (uca != 0) {
					fs = ucfind_firt_set(uca);
					result->smallfs += fs;
					result->finish = 1;
					return;
				}
				result->smallfs += 8;
				a++;
			}
			result->smallfs += 16;
			lenbyte -= 2;
			a += 2;
		}
	case 1:
		while (lenbyte >= 1) {
			uca = *a; a++;
			if (uca != 0) {
				fs = ucfind_firt_set(uca);
				result->smallfs += fs;
				result->finish = 1;
				return;
			}
			result->smallfs += 8;
			lenbyte--;
		}
		break;
	default:
		spt_print("\n%s\t%d", __func__, __LINE__);
		spt_assert(0);

	}

	if (bitend) {
		uca = *a;
		fs = ucfind_firt_set(uca);
		if (fs >= bitend)
			result->smallfs += bitend;
		else
			result->smallfs += fs;
	}
}

int align_compare(u8 *a, u8 *b, s64 len, int align, struct vec_cmpret_t *result)
{
	u8 uca, ucb, ucres, bitend;
	u64 ulla, ullb, fs;
	s64 lenbyte;
	u32 uia, uib;
	u16 usa, usb;
	int ret = 0;
	u8 *c;

	bitend = len%8;
	lenbyte = len/8;

	switch (align) {
	case 8:
		while (lenbyte >= 8) {
			ulla = *(u64 *)a;
			ullb = *(u64 *)b;

			if (ulla == ullb) {
				result->pos += 64;
				lenbyte -= 8;
			} else
				goto perbyte;

			a += 8;
			b += 8;
		}
	case 4:
		while (lenbyte >= 4) {
			uia = *(u32 *)a;
			uib = *(u32 *)b;

			if (uia == uib) {
				result->pos += 32;
				lenbyte -= 4;
			} else
				goto perbyte;

			a += 4;
			b += 4;
		}

	case 2:
		while (lenbyte >= 2) {
			usa = *(u16 *)a;
			usb = *(u16 *)b;

			if (usa == usb) {
				result->pos += 16;
				lenbyte -= 2;
			} else
				goto perbyte;
			a += 2;
			b += 2;
		}
perbyte:
	case 1:
		while (lenbyte >= 1) {
			uca = *(u64 *)a;
			ucb = *(u64 *)b;

			if (uca == ucb) {
				result->pos += 8;
				lenbyte--;
			} else {
				ucres = uca^ucb;
				fs = ucfind_firt_set(ucres);
				result->smallfs = result->pos;
				result->pos += fs;

				if (uca > ucb) {
					ret = 1;
					c = b;
					uca = ucb;
				} else {
					ret = -1;
					c = a;
				}
				uca = uca << fs;
				if (uca != 0) {
					fs = ucfind_firt_set(uca);
					result->smallfs =
						result->pos + fs;
					result->finish = 1;
				} else {
					result->smallfs += 8;
					c++;
					lenbyte--;
					find_smallfs(c,
							(lenbyte<<3)+bitend,
							1, result);
					//result->finish = 1;
				}
				return ret;
			}
			a++;
			b++;
		}
		break;
	default:
		spt_print("\n%s\t%d", __func__, __LINE__);
		spt_assert(0);
	}

	if (bitend) {
		uca = *a;
		ucb = *b;
		uca = uca >> (8-bitend) << (8-bitend);
		ucb = ucb >> (8-bitend) << (8-bitend);
		if (uca == ucb) {
			result->pos += bitend;
			result->smallfs = result->pos;
			ret = 0;
		} else {
			ucres = uca^ucb;
			fs = ucfind_firt_set(ucres);
			result->smallfs = result->pos;
			result->pos += fs;

			if (uca > ucb) {
				ret = 1;
				c = b;
				uca = ucb;
			} else {
				ret = -1;
				c = a;
			}
			uca = uca << fs;
			if (uca != 0) {
				fs = ucfind_firt_set(uca);
				result->smallfs = result->pos + fs;
				result->finish = 1;
			} else {
				result->smallfs += bitend;
			//	result->finish = 1;
			}

		}
	}
	return ret;
}

u64 find_fs(char *a, u64 start, u64 len)
{
	s64 align;
	u64 fs, ret, ulla;
	s64 lenbyte;
	u32 uia;
	u16 usa;
	u8 uca, bitstart, bitend;
	u8 *acstart;
	int i;

	fs = 0;
	bitstart = start%8;
	bitend = (bitstart + len)%8;
	acstart = (u8 *)a + start/8;
	lenbyte =  (bitstart + len)/8;
	ret = start;

	if (lenbyte == 0) {
		uca = *acstart;
		uca &= (1<<(8-bitstart))-1;
		fs = ucfind_firt_set(uca);
		if (fs >= bitend)
			return start + len;
		else
			return start + fs - bitstart;
	}

	if (bitstart != 0) {
		lenbyte--;
		uca = *acstart; acstart++;
		uca &= (1<<(8-bitstart))-1;
		if (uca == 0)
			ret += 8 - bitstart;
		else {
			fs = ucfind_firt_set(uca);
			ret += fs-bitstart;
			return ret;
		}
	}

	if (((unsigned long)acstart%8) != 0) {
		align = (unsigned long)acstart%8;
		align = 8-align;
		if (lenbyte < align) {
			while (lenbyte >= 1) {
				uca = *acstart;
				acstart++;
				if (uca != 0) {
					fs = ucfind_firt_set(uca);
					ret += fs;
					return ret;
				}
				ret += 8;
				lenbyte--;
			}
		}else {
			lenbyte -= align;
			while (align >= 1) {
				uca = *acstart;
				acstart++;
				if (uca != 0) {
					fs = ucfind_firt_set(uca);
					ret += fs;
					return ret;
				}
				ret += 8;
				align--;
			}
		}
	}

	while (lenbyte >= 8) {
		ulla = *(u64 *)acstart;
		if (ulla != 0) {
			for (i = 0; i < 8; i++) {
				uca = *acstart;
				if (uca != 0) {
					fs = ucfind_firt_set(uca);
					ret += fs;
					return ret;
				}
				ret += 8;
				acstart++;
			}
		}
		ret += 64;
		lenbyte -= 8;
		acstart += 8;
	}
	while (lenbyte >= 4) {
		uia = *(u32 *)acstart;
		if (uia != 0) {
			for (i = 0; i < 4; i++) {
				uca = *acstart;
				if (uca != 0) {
					fs = ucfind_firt_set(uca);
					ret += fs;
					return ret;
				}
				ret += 8;
				acstart++;
			}
		}
		ret += 32;
		lenbyte -= 4;
		acstart += 4;
	}
	while (lenbyte >= 2) {
		usa = *(u16 *)acstart;
		if (usa != 0) {
			for (i = 0; i < 2; i++) {
				uca = *acstart;
				if (uca != 0) {
					fs = ucfind_firt_set(uca);
					ret += fs;
					return ret;
				}
				ret += 8;
				acstart++;
			}
		}
		ret += 16;
		lenbyte -= 2;
		acstart += 2;
	}
	while (lenbyte >= 1) {
		uca = *acstart;
		if (uca != 0) {
			fs = ucfind_firt_set(uca);
			ret += fs;
			return ret;
		}
		ret += 8;
		lenbyte--;
		acstart++;
	}
	if (bitend) {
		uca = *acstart;
		fs = ucfind_firt_set(uca);
		if (fs >= bitend)
			ret += bitend;
		else
			ret += fs;
	}
	return ret;
}

#endif

#if 1
/*bitend: not include*/
int bit_inbyte_cmp(u8 *a, u8 *b, u8 bitstart, u8 bitend,
		struct vec_cmpret_t *result)
{
	u8 uca, ucb, cres, fs;
	int ret;

	uca = *a;
	ucb = *b;

	uca = (u8)(uca << bitstart) >> bitstart;
	ucb = (u8)(ucb << bitstart) >> bitstart;

	uca = (u8)(uca >> (8-bitend)) << (8-bitend);
	ucb = (u8)(ucb >> (8-bitend)) << (8-bitend);

	if (uca == ucb) {
		result->pos += bitend - bitstart;
		ret = 0;
	} else {
		cres = uca^ucb;
		fs = ucfind_firt_set(cres);
		result->pos += fs-bitstart;
		if (uca > ucb) {
			uca = ucb;
			ret = 1;
		} else {
			ret = -1;
		}
		result->smallfs = result->pos;
		uca = uca << fs;
		bitend = bitend - fs;
		fs = ucfind_firt_set(uca);
		if (fs == 8)
			result->smallfs += bitend;
		else
			result->smallfs += fs;
	}

	result->finish = 1;
	return ret;
}
/**
 * diff_identify - compare a and b starting from the start bit,
 * a total comparison len bit.
 * @a: pointer of data to compare
 * @b: pointer of data to compare
 * @start: compare starting from @start bit
 * @len: how many bits are compared
 * @result: return info
 * return 1(a > b);0(a==b); < 0(a < b)
 */
int diff_identify(char *a, char *b, u64 start, u64 len,
		struct vec_cmpret_t *result)
{
	s64 align;
	u64 fz, fs;
	s64 lenbyte;
	u8 uca, ucb, cres, bitstart, bitend;
	u8 *acstart;
	u8 *bcstart;
	int ret = 0;

//	perbyteCnt = perllCnt = perintCnt = pershortCnt = 0;
	fs = fz = 0;
	bitstart = start%8;
	bitend = (bitstart + len)%8;
	acstart = (u8 *)a + start/8;
	bcstart = (u8 *)b + start/8;
	lenbyte =  (bitstart + len)/8;
	result->pos = start;
	result->finish = 0;

	if (lenbyte == 0)
		return bit_inbyte_cmp(acstart, bcstart,
				bitstart, bitend, result);

	if (bitstart != 0) {
		lenbyte--;
		uca = *acstart; acstart++;
		ucb = *bcstart; bcstart++;
		uca = uca << bitstart;
		uca = uca >> bitstart;
		ucb = ucb << bitstart;
		ucb = ucb >> bitstart;

		if (uca == ucb)
			result->pos += 8-bitstart;
		else {
			cres = uca^ucb;
			fs = ucfind_firt_set(cres);
			result->pos += fs-bitstart;
			if (uca > ucb) {
				ret =  1;
				acstart = bcstart;
				uca = ucb;
			} else
				ret = -1;
			result->smallfs = result->pos;

			uca = uca << fs;
			if (uca == 0)
				result->smallfs += 8 - fs;
			else {
				fs = ucfind_firt_set(uca);
				result->smallfs += fs;
				result->finish = 1;
				return ret;
			}
			if (((unsigned long)acstart%8) != 0) {
				align = (unsigned long)acstart%8;
				align = 8-align;
				if (lenbyte < align)
					find_smallfs(acstart,
							(lenbyte<<3)+bitend,
							1, result);
				else {
					find_smallfs(acstart,
							align<<3,
							1, result);

					if ((result->finish != 1) &&
						(result->smallfs <
						 start + len)) {
						acstart += align;
						lenbyte -= align;
						find_smallfs(
								acstart,
							(lenbyte<<3)+bitend,
								8, result);
					}
				}
			} else
				find_smallfs(acstart,
						(lenbyte<<3)+bitend,
						8, result);

			result->finish = 1;
			return ret;
		}

	}

	if ((unsigned long)acstart%8 == (unsigned long)bcstart%8) {
		if (((unsigned long)acstart%8) != 0) {
			align = (unsigned long)acstart%8;
			align = 8-align;
			if (lenbyte < align) {
				ret = align_compare(acstart, bcstart,
						(lenbyte<<3) + bitend,
						1, result);
			} else {
				ret = align_compare(acstart, bcstart,
						align<<3, 1, result);
				if (result->finish == 1)
					return ret;

				acstart += align;
				bcstart += align;
				lenbyte -= align;
				if (ret == 0)
					ret = align_compare(acstart,
							bcstart,
							(lenbyte<<3) + bitend,
							8, result);
				else if (ret == 1)
					find_smallfs(bcstart,
							(lenbyte<<3) + bitend,
							8, result);
				else
					find_smallfs(acstart,
							(lenbyte<<3) + bitend,
							8, result);
			}
		} else
			ret = align_compare(acstart, bcstart,
					(lenbyte<<3) + bitend,
					8, result);

	} else if ((unsigned long)acstart%4 == (unsigned long)bcstart%4) {
		if (((unsigned long)acstart%4) != 0) {
			align = (unsigned long)acstart%4;
			align = 4-align;
			if (lenbyte < align)
				ret = align_compare(acstart, bcstart,
						(lenbyte<<3) + bitend,
						1, result);
			else {
				ret = align_compare(acstart, bcstart,
						align<<3, 1, result);
				if (result->finish == 1)
					return ret;

				acstart += align;
				bcstart += align;
				lenbyte -= align;
				if (ret == 0)
					ret = align_compare(acstart,
							bcstart,
							(lenbyte<<3) + bitend,
							8, result);
				else if (ret == 1)
					find_smallfs(bcstart,
							(lenbyte<<3) + bitend,
							4, result);
				else
					find_smallfs(acstart,
							(lenbyte<<3) + bitend,
							4, result);
			}
		} else
			ret = align_compare(acstart, bcstart,
					(lenbyte<<3) + bitend,
					4, result);

	} else if ((unsigned long)acstart%2 == (unsigned long)bcstart%2) {
		if (((unsigned long)acstart%2) != 0) {
			align = (unsigned long)acstart%2;
			align = 1;
			if (lenbyte < align)
				ret = align_compare(acstart, bcstart,
						(lenbyte<<3) + bitend,
						1, result);
			else {
				ret = align_compare(acstart, bcstart,
						align<<3, 1, result);
				if (result->finish == 1)
					return ret;

				acstart += align;
				bcstart += align;
				lenbyte -= align;
				if (ret == 0)
					ret = align_compare(acstart, bcstart,
						(lenbyte<<3) + bitend,
						2, result);
				else if (ret == 1)
					find_smallfs(
						bcstart,
						(lenbyte<<3) + bitend,
						2, result);
				else
					find_smallfs(
						acstart,
						(lenbyte<<3) + bitend,
						2, result);
			}
		} else
			ret = align_compare(acstart, bcstart,
					(lenbyte<<3) + bitend, 2, result);
	} else
		ret = align_compare(acstart, bcstart,
				(lenbyte<<3) + bitend, 1, result);

	result->finish = 1;
	return ret;
}
#endif

void debug_get_final_vec(struct cluster_head_t *pclst, struct spt_vec *pvec,
		struct spt_vec_f *pvec_f, u64 sp,
		u32 data, int direction)
{
	if (pvec->type == SPT_VEC_SIGNPOST) {
		if (pvec->ext_sys_flg == SPT_VEC_SYS_FLAG_DATA)
			spt_debug("found SPT_VEC_SYS_FLAG_DATA vec\r\n");

		pvec_f->data = data;
		pvec_f->pos = pvec->idx << SPT_VEC_SIGNPOST_BIT;
		if (direction == SPT_DOWN) {
			pvec_f->down = pvec->rd;
			pvec_f->right = SPT_INVALID;
		} else {
			pvec_f->right = pvec->rd;
			pvec_f->down = SPT_INVALID;
		}
		return;
	}

	pvec_f->down = pvec->down;
	pvec_f->pos = pvec->pos + sp;
	if (pvec->type == SPT_VEC_DATA) {
		pvec_f->data = pvec->rd;
		pvec_f->right = SPT_NULL;
		return;
	}
	pvec_f->right = pvec->rd;
	if (data != SPT_INVALID)
		pvec_f->data = data;
	else {
		pvec_f->data = get_data_id(pclst, pvec);
		if (pvec_f->data == SPT_DO_AGAIN)
			spt_debug("invalid found\r\n");
	}
}

void debug_vec_print(struct spt_vec_f *pvec_f, int vec_id)
{
	spt_print("{down:%d, right:%d, pos:%lld, data:%d}[%d]\r\n",
			pvec_f->down, pvec_f->right,
			pvec_f->pos, pvec_f->data, vec_id);
}

void debug_id_vec_print(struct cluster_head_t *pclst, int id)
{
}
void debug_dh_ext_print(struct spt_dh_ext *p)
{
	spt_print("hang_vec_id:%d\tlower_cluster:%p\r\n",
			p->hang_vec, p->plower_clst);
}
void debug_data_print(char *pdata)
{
#if 0
	int i;
	u8 *p = (u8 *)pdata;

	for (i = 0; i < DATA_SIZE; i++, p++) {
		if (i%8 == 0)
			spt_print("\r\n");
		spt_print("%02x ", *p);
	}
	spt_print("\r\n");
#endif
	spt_print("print data addr is %p\r\n", pdata);
}
void debug_pdh_data_print(struct cluster_head_t *pclst, struct spt_dh *pdh)
{
	struct spt_dh_ext *pdh_ext;

	if (pclst->is_bottom)
		debug_data_print(pdh->pdata);
	else {
		pdh_ext = (struct spt_dh_ext *)pdh->pdata;
		debug_data_print(pdh_ext->data);
	}
}
struct travl_info *debug_travl_stack_pop(struct spt_stack *p_stack)
{
	return (struct travl_info *)spt_stack_pop(p_stack);
}


void debug_travl_stack_destroy(struct spt_stack *p_stack)
{
	struct travl_info *node;

	while (!spt_stack_empty(p_stack)) {
		node = debug_travl_stack_pop(p_stack);
		spt_free(node);
	}
	spt_stack_destroy(p_stack);
}


void debug_travl_stack_push(struct spt_stack *p_stack,
		struct spt_vec_f *pvec_f,
		long long signpost)
{
	struct travl_info *node;

	node = (struct travl_info *)spt_malloc(sizeof(struct travl_info));
	if (node == NULL) {
		spt_print("\r\nOUT OF MEM %d\t%s",
				__LINE__, __func__);
		debug_travl_stack_destroy(p_stack);
		return;
	}
	node->signpost = signpost;
	node->vec_f = *pvec_f;
	spt_stack_push(p_stack, node);
}


void debug_cluster_travl(struct cluster_head_t *pclst)
{
	struct spt_stack stack = {0};
	struct spt_stack *pstack = &stack;
	//u8 data[DATA_SIZE] = {0};
	int cur_data, cur_vecid;
	struct spt_vec *pcur;
	struct spt_vec_f st_vec_f;
	struct spt_dh *pdh;
	char *pcur_data = NULL;
	u64 signpost;
	struct travl_info *pnode;
	u32 ref_total;
	char *data;

	ref_total = 0;
	signpost = 0;
	spt_stack_init(pstack, 1000);
	cur_data = SPT_INVALID;

	cur_vecid = pclst->vec_head;
	pcur = (struct spt_vec *)vec_id_2_ptr(pclst, pclst->vec_head);

	debug_get_final_vec(pclst, pcur, &st_vec_f,
			signpost, cur_data, SPT_RIGHT);
	if (pcur->down == SPT_NULL && pcur->rd == SPT_NULL) {
		spt_print("cluster is null\r\n");
		debug_travl_stack_destroy(pstack);
		return;
	}

	cur_data = st_vec_f.data;
	if (cur_data != SPT_NULL) {
		pdh = (struct spt_dh *)db_id_2_ptr(pclst, cur_data);
		pcur_data = pdh->pdata;
		ref_total += pdh->ref;
	}
	debug_vec_print(&st_vec_f, cur_vecid);
#if 0
	if (st_vec_r.pos == 0) {
		spt_print("only one vec in this cluster\r\n");
		debug_vec_print(&st_vec_r, cur_vec);
		debug_data_print(pcur_data);
		return;
	}
#endif
	debug_travl_stack_push(pstack, &st_vec_f, signpost);

	while (1) {
		if (pcur->type != SPT_VEC_DATA) {
			cur_vecid = st_vec_f.right;
			pcur = (struct spt_vec *)vec_id_2_ptr(pclst,
					cur_vecid);
			debug_get_final_vec(pclst, pcur,
					&st_vec_f, signpost,
					cur_data, SPT_RIGHT);
			debug_vec_print(&st_vec_f, cur_vecid);
			if (pcur->type == SPT_VEC_SIGNPOST) {
				signpost = st_vec_f.pos;

				cur_vecid = st_vec_f.right;
				pcur = (struct spt_vec *)vec_id_2_ptr(pclst,
						cur_vecid);
				debug_get_final_vec(pclst, pcur,
						&st_vec_f, signpost,
						cur_data, SPT_RIGHT);
				debug_vec_print(&st_vec_f, cur_vecid);
				if (pcur->type == SPT_VEC_SIGNPOST)
					spt_debug("double signpost vec found!!\r\n");
			}
			debug_travl_stack_push(pstack, &st_vec_f, signpost);
		} else {
			if (pcur_data != NULL) {
				data = get_real_data(pclst, pcur_data);
				if (!pclst->is_bottom)
					debug_dh_ext_print(
						(struct spt_dh_ext *)pcur_data);
				debug_data_print(data);
				if (pclst->is_bottom) {
					unsigned char *data_mem = NULL;

					data_mem = pclst->get_key_in_tree(data);
					debug_data_print(data_mem);
					pclst->get_key_in_tree_end(data);
				}
			}

			if (spt_stack_empty(pstack))
				break;

			while (1) {
				pnode = debug_travl_stack_pop(pstack);
				if (pnode == (struct travl_info *)-1) {
					spt_print("\r\n");
					debug_travl_stack_destroy(pstack);
					spt_print("\r\n data_total:%d\t"
							"ref_total:%d\r\n",
					pclst->data_total, ref_total);
					return;
				}
				signpost = pnode->signpost;
				if (pnode->vec_f.down != SPT_NULL) {
					cur_vecid = pnode->vec_f.down;
					pcur = (struct spt_vec *)
						vec_id_2_ptr(pclst, cur_vecid);
					debug_get_final_vec(pclst, pcur,
							&st_vec_f, signpost,
							SPT_INVALID,
							SPT_DOWN);
					//debug_vec_print(&st_vec_f, cur_vecid);

					cur_data = st_vec_f.data;
					pdh = (struct spt_dh *)
						db_id_2_ptr(pclst, cur_data);
					pcur_data = pdh->pdata;
					ref_total += pdh->ref;
					spt_print("\r\n@@data[%p],bit:%lld\r\n",
							pcur_data,
							st_vec_f.pos);
					debug_vec_print(&st_vec_f, cur_vecid);
					debug_travl_stack_push(pstack,
							&st_vec_f, signpost);
					spt_free(pnode);
					break;
				}
				spt_free(pnode);
			}
		}
	}
	debug_travl_stack_destroy(pstack);
	spt_print("\r\n@@@@@@@@ data_total:%d\tref_total:%d@@@@@@@@\r\n",
	pclst->data_total, ref_total);
}

u32 debug_thrd_vec_statistic(struct cluster_head_t *pclst)
{
	int i;
	u32 total = 0;

	for (i = 0; i < pclst->thrd_total; i++)
		total += pclst->thrd_data[i].vec_cnt;
	return total;
}

u32 debug_thrd_data_statistic(struct cluster_head_t *pclst)
{
	int i;
	u32 total = 0;

	for (i = 0; i < pclst->thrd_total; i++)
		total += pclst->thrd_data[i].data_cnt;
	return total;
}
int debug_statistic2(struct cluster_head_t *pclst)
{
	struct spt_vec **stack;
	int cur_data, cur_vecid, index;
	struct spt_vec *pcur, cur_vec;
	struct spt_dh *pdh;
	u32 ref_total, buf_vec_total, buf_data_total, data_total;
	u32 lower_ref;
	struct cluster_head_t *plower_clst;
	char *pcur_data = NULL;

	buf_vec_total = 0;
	buf_data_total = 0;
	data_total = 0;
	ref_total = 0;
	lower_ref = 0;

	stack = (struct spt_vec **)spt_malloc(4096*8*8);
	if (stack == NULL)
		return 0;
	index = 0;

	cur_data = SPT_INVALID;

	cur_vecid = pclst->vec_head;
	pcur = (struct spt_vec *)vec_id_2_ptr(pclst, pclst->vec_head);

	cur_vec.val = pcur->val;
	if (cur_vec.down == SPT_NULL && cur_vec.rd == SPT_NULL) {
		spt_print("cluster is null\r\n");
		return 0;
	}
	stack[index] = pcur;
	index++;

	while (1) {
		if (cur_vec.type != SPT_VEC_DATA) {
			cur_vecid = cur_vec.rd;
			pcur = (struct spt_vec *)vec_id_2_ptr(pclst, cur_vecid);
			cur_vec.val = pcur->val;

			stack[index] = pcur;
			index++;
		} else {
			cur_data = cur_vec.rd;
			if (cur_data != SPT_NULL) {
				pdh = (struct spt_dh *)db_id_2_ptr(pclst,
						cur_data);
				pcur_data = pdh->pdata;
				ref_total += pdh->ref;
				if (!pclst->is_bottom) {
					plower_clst = ((struct spt_dh_ext *)
							pcur_data)->plower_clst;
					buf_data_total +=
					debug_thrd_data_statistic(plower_clst);
					buf_vec_total +=
					debug_thrd_vec_statistic(plower_clst);
					lower_ref +=
						debug_statistic2(plower_clst);
					data_total += plower_clst->data_total;
				}

			}

			if (index == 0)
				break;

			while (1) {
				index--;
				pcur = stack[index];
				cur_vec.val = pcur->val;
				if (cur_vec.down != SPT_NULL) {
					cur_vecid = cur_vec.down;
					pcur = (struct spt_vec *)
						vec_id_2_ptr(pclst, cur_vecid);
					cur_vec.val = pcur->val;
					stack[index] = pcur;
					index++;
					break;
				}
				if (index == 0) {
					//todo
					goto sort_exit;
				}
			}
		}
	}
sort_exit:
	spt_free(stack);
	if (!pclst->is_bottom) {
		spt_debug("\r\n lower_total_ref:%d\r\n", lower_ref);
		spt_debug("\r\n data_total:%d\r\n", data_total);
		spt_debug("\r\n buf_data_total:%d\r\n", buf_data_total);
		spt_debug("\r\n buf_vec_total:%d\r\n", buf_vec_total);
	}
	return ref_total;
}


int debug_statistic(struct cluster_head_t *pclst)
{
	struct spt_stack stack = {0};
	struct spt_stack *pstack = &stack;
	int cur_data, cur_vecid;
	struct spt_vec *pcur;
	struct spt_vec_f st_vec_f;
	struct spt_dh *pdh;
	char *pcur_data = NULL;
	u64 signpost;
	struct travl_info *pnode;
	u32 ref_total, buf_vec_total, buf_data_total, data_total;
	u32 lower_ref;
	char *data;
	struct cluster_head_t *plower_clst;

	buf_vec_total = 0;
	buf_data_total = 0;
	data_total = 0;
	ref_total = 0;
	lower_ref = 0;
	signpost = 0;
	spt_stack_init(pstack, 1000);
	cur_data = SPT_INVALID;

	cur_vecid = pclst->vec_head;
	pcur = (struct spt_vec *)vec_id_2_ptr(pclst, pclst->vec_head);

	debug_get_final_vec(pclst, pcur, &st_vec_f,
			signpost, cur_data, SPT_RIGHT);
	if (pcur->down == SPT_NULL && pcur->rd == SPT_NULL) {
		debug_travl_stack_destroy(pstack);
		return ref_total;
	}

	cur_data = st_vec_f.data;
	if (cur_data != SPT_NULL) {
		pdh = (struct spt_dh *)db_id_2_ptr(pclst, cur_data);
		pcur_data = pdh->pdata;
		ref_total += pdh->ref;
	}
#if 0
	if (st_vec_r.pos == 0) {
		printf("only one vec in this cluster\r\n");
		debug_vec_print(&st_vec_r, cur_vec);
		debug_data_print(pcur_data);
		return;
	}
#endif
	debug_travl_stack_push(pstack, &st_vec_f, signpost);

	while (1) {
		if (pcur->type != SPT_VEC_DATA) {
			cur_vecid = st_vec_f.right;
			pcur = (struct spt_vec *)vec_id_2_ptr(pclst,
					cur_vecid);
			debug_get_final_vec(pclst, pcur, &st_vec_f,
					signpost, cur_data, SPT_RIGHT);

			if (pcur->type == SPT_VEC_SIGNPOST) {
				signpost = st_vec_f.pos;

				cur_vecid = st_vec_f.right;
				pcur = (struct spt_vec *)vec_id_2_ptr(pclst,
						cur_vecid);

				debug_get_final_vec(pclst, pcur, &st_vec_f,
						signpost, cur_data, SPT_RIGHT);

				if (pcur->type == SPT_VEC_SIGNPOST)
					spt_debug("double signpost vec found!!\r\n");
			}
			debug_travl_stack_push(pstack, &st_vec_f, signpost);
		} else {
			spt_thread_start(g_thrd_id);
			spt_thread_exit(g_thrd_id);
			if (pcur_data != NULL) {
				data = get_real_data(pclst, pcur_data);
				if (!pclst->is_bottom) {
					plower_clst = ((struct spt_dh_ext *)
							pcur_data)->plower_clst;

					buf_data_total +=
					debug_thrd_data_statistic(plower_clst);

					buf_vec_total +=
					debug_thrd_vec_statistic(plower_clst);

					lower_ref +=
						debug_statistic(plower_clst);
					data_total +=
						plower_clst->data_total;
				}
			}

			if (spt_stack_empty(pstack))
				break;

			while (1) {
				pnode = debug_travl_stack_pop(pstack);
				if (pnode == (struct travl_info *)-1) {
					debug_travl_stack_destroy(pstack);
					if (pclst->is_bottom)
						return ref_total;

					spt_debug("\r\n lower_total_ref:%d\r\n",
								lower_ref);
					spt_debug("\r\n data_total:%d\r\n",
								data_total);
					spt_debug("\r\n buf_data_total:%d\r\n",
								buf_data_total);
					spt_debug("\r\n buf_vec_total:%d\r\n",
								buf_vec_total);
					return ref_total;
				}
				signpost = pnode->signpost;
				if (pnode->vec_f.down != SPT_NULL) {
					cur_vecid = pnode->vec_f.down;
					pcur = (struct spt_vec *)
						vec_id_2_ptr(pclst, cur_vecid);

					debug_get_final_vec(pclst, pcur,
							&st_vec_f, signpost,
							SPT_INVALID, SPT_DOWN);

					cur_data = st_vec_f.data;
					pdh = (struct spt_dh *)
						db_id_2_ptr(pclst, cur_data);
					pcur_data = pdh->pdata;
					ref_total += pdh->ref;

					debug_travl_stack_push(pstack,
							&st_vec_f, signpost);
					spt_free(pnode);
					break;
				}
				spt_free(pnode);
			}
		}
	}
	debug_travl_stack_destroy(pstack);
	if (!pclst->is_bottom) {
		spt_debug("\r\n lower_total_ref:%d\r\n", lower_ref);
		spt_debug("\r\n data_total:%d\r\n", data_total);
		spt_debug("\r\n buf_data_total:%d\r\n", buf_data_total);
		spt_debug("\r\n buf_vec_total:%d\r\n", buf_vec_total);
	}
	return ref_total;
}

void debug_buf_free(struct cluster_head_t *pclst)
{
	struct spt_stack stack = {0};
	struct spt_stack *pstack = &stack;
	int cur_data, cur_vecid, i;
	struct spt_vec *pcur;
	struct spt_vec_f st_vec_f;
	struct spt_dh *pdh;
	char *pcur_data = NULL;
	u64 signpost;
	struct travl_info *pnode;
	u32 ref_total;
	char *data;
	struct cluster_head_t *plower_clst;

	signpost = 0;
	spt_stack_init(pstack, 1000);
	cur_data = SPT_INVALID;

	cur_vecid = pclst->vec_head;
	pcur = (struct spt_vec *)vec_id_2_ptr(pclst, pclst->vec_head);

	debug_get_final_vec(pclst, pcur, &st_vec_f,
			signpost, cur_data, SPT_RIGHT);
	if (pcur->down == SPT_NULL && pcur->rd == SPT_NULL) {
		debug_travl_stack_destroy(pstack);
		return;
	}

	cur_data = st_vec_f.data;
	if (cur_data != SPT_NULL) {
		pdh = (struct spt_dh *)db_id_2_ptr(pclst, cur_data);
		pcur_data = pdh->pdata;
	}
#if 0
	if (st_vec_r.pos == 0) {
		printf("only one vec in this cluster\r\n");
		debug_vec_print(&st_vec_r, cur_vec);
		debug_data_print(pcur_data);
		return;
	}
#endif
	debug_travl_stack_push(pstack, &st_vec_f, signpost);
	while (1) {
		if (pcur->type != SPT_VEC_DATA) {
			cur_vecid = st_vec_f.right;
			pcur = (struct spt_vec *)vec_id_2_ptr(pclst, cur_vecid);
			debug_get_final_vec(pclst, pcur, &st_vec_f,
					signpost, cur_data, SPT_RIGHT);

			if (pcur->type == SPT_VEC_SIGNPOST) {
				signpost = st_vec_f.pos;

				cur_vecid = st_vec_f.right;
				pcur = (struct spt_vec *)vec_id_2_ptr(pclst,
						cur_vecid);
				debug_get_final_vec(pclst, pcur,
						&st_vec_f, signpost,
						cur_data, SPT_RIGHT);

				if (pcur->type == SPT_VEC_SIGNPOST)
					spt_debug("double signpost vec found!!\r\n");
			}
			debug_travl_stack_push(pstack, &st_vec_f, signpost);
		} else {
			if (pcur_data != NULL) {
				data = get_real_data(pclst, pcur_data);
				if (!pclst->is_bottom) {
					int thrd_num;

					plower_clst = ((struct spt_dh_ext *)
							pcur_data)->plower_clst;
					thrd_num = plower_clst->thrd_total;
					for (i = 0; i < thrd_num; i++) {
						vec_free(plower_clst, i);
						db_free(plower_clst, i);
					}
					plower_clst->status = SPT_OK;
				}
			}

			if (spt_stack_empty(pstack))
				break;

			while (1) {
				pnode = debug_travl_stack_pop(pstack);
				if (pnode == (struct travl_info *)-1) {
					debug_travl_stack_destroy(pstack);
					return;
				}
				signpost = pnode->signpost;
				if (pnode->vec_f.down != SPT_NULL) {
					cur_vecid = pnode->vec_f.down;
					pcur = (struct spt_vec *)vec_id_2_ptr(
							pclst,
							cur_vecid);

					debug_get_final_vec(pclst, pcur,
							&st_vec_f, signpost,
							SPT_INVALID, SPT_DOWN);
					//debug_vec_print(&st_vec_f, cur_vecid);

					cur_data = st_vec_f.data;
					pdh = (struct spt_dh *)
						db_id_2_ptr(pclst, cur_data);
					pcur_data = pdh->pdata;
					ref_total += pdh->ref;
					debug_travl_stack_push(pstack,
							&st_vec_f, signpost);
					spt_free(pnode);
					break;
				}
				spt_free(pnode);
			}
		}
	}
	debug_travl_stack_destroy(pstack);
}
void debug_cluster_info_show(struct cluster_head_t *pclst)
{
	int data_cnt, vec_cnt;

	data_cnt = debug_thrd_data_statistic(pclst);
	vec_cnt = debug_thrd_vec_statistic(pclst);
	spt_print("%p [data_buf]:%d [vec_buf]:%d [vec_used]:%d\t"
	"[data_used]:%d\r\n",
	pclst, data_cnt, vec_cnt, pclst->used_vec_cnt,
	pclst->used_dblk_cnt);
}

void debug_lower_cluster_info_show(void)
{
	struct list_head *list_itr;
	struct cluster_head_t *pclst;
	int i = 0;

	spt_print("\r\n==========cluster info show=====================\r\n");
	list_for_each(list_itr, &pgclst->c_list) {
		pclst = list_entry(list_itr, struct cluster_head_t, c_list);
		spt_print("[cluster %d]", i);
		debug_cluster_info_show(pclst);
		i++;
	}
	spt_print("\r\n==========cluster info end======================\r\n");
}
void clean_lower_cluster_cache(void)
{
	struct list_head *list_itr;
	struct cluster_head_t *pclst;
	int j;

	list_for_each(list_itr, &pgclst->c_list) {
		pclst = list_entry(list_itr, struct cluster_head_t, c_list);
		spt_thread_wait(1000, 60);
		for (j = 0; j < pclst->thrd_total; j++) {
			vec_free(pclst, j);
			db_free(pclst, j);
		}
	}
}
