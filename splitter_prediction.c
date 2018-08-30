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

#define SPT_PREDICTION_ERR -1
#define SPT_PREDICTION_OK  0
int total_data_num = 0;
extern int sd_perf_debug;
int test_bit_set(char *pdata, u64 test_bit)
{
	u64 byte_offset = test_bit/8;
	u8  bit_offset = test_bit % 8;
	char* byte_start = pdata + byte_offset;
	if(*byte_start & (1 << (8 - bit_offset  - 1)))
		return 1;
	return 0;
}
int test_bit_zero(char *pdata, u64 startbit, u64 lenbit)
{
	s64 align;
	u64 ulla;
	s64 lenbyte;
	u32 uia;
	u16 usa;
	u8 uca, bitstart, bitend;
	u8 *acstart;
	int i;

	bitstart = startbit%8;
	bitend = (bitstart + lenbit)%8;
	acstart = (u8 *)pdata + startbit/8;
	lenbyte =  (bitstart + lenbit)/8;

	if (lenbyte == 0) {
		uca = *acstart;
		uca &= (1<<(8-bitstart))-1;
		if (uca >> (8 - bitend))
			return 0;
		else
			return 1;
	}

	if (bitstart != 0) {
		lenbyte--;
		uca = *acstart; acstart++;
		uca &= (1<<(8-bitstart))-1;
		if (uca != 0)
			return 0;
	}

	if (((unsigned long)acstart%8) != 0) {
		align = (unsigned long)acstart%8;
		align = 8-align;
		if (lenbyte < align) {
			while (lenbyte >= 1) {
				uca = *acstart;
				acstart++;
				if (uca != 0)
					return 0;
				lenbyte--;
			}
		} else {

			lenbyte -= align;
			while (align >= 1) {
				uca = *acstart;
				acstart++;
				if (uca != 0)
					return 0;
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
					return 0;
				}
				acstart++;
			}
		}
		lenbyte -= 8;
		acstart += 8;
	}
	while (lenbyte >= 4) {
		uia = *(u32 *)acstart;
		if (uia != 0) {
			for (i = 0; i < 4; i++) {
				uca = *acstart;
				if (uca != 0) {
					return 0;
				}
				acstart++;
			}
		}
		lenbyte -= 4;
		acstart += 4;
	}
	while (lenbyte >= 2) {
		usa = *(u16 *)acstart;
		if (usa != 0) {
			for (i = 0; i < 2; i++) {
				uca = *acstart;
				if (uca != 0) {
					return 0;
				}
				acstart++;
			}
		}
		lenbyte -= 2;
		acstart += 2;
	}
	while (lenbyte >= 1) {
		uca = *acstart;
		if (uca != 0) {
			return 0;
		}
		lenbyte--;
		acstart++;
	}
	if (bitend) {
		uca = *acstart >> (8 - bitend);
		if(uca)
			return 0;
		else
			return 1;
	}
	return 1;
}

int get_change_bit_byte(char src, char dst)
{
	int i = 0;
	u8 checkbyte = src ^ dst;
	if (checkbyte == 0)
		return -1;

	for (i = 7; i >= 0; i--)
		if(checkbyte >> i != 0)
			return 7 - i;
}

/* if  equal return -1 */
int get_first_change_bit(char *src, char *dst, u64 startbit, u64 endbit)
{
	s64 align;
	u64 ret, ulla, ullda;
	s64 lenbyte;
	u32 uia, uida;
	u16 usa, usda;
	u8 uca,ucda, bitstart, bitend;
	u8 *srcstart, *dststart;
	int i;
	int changebit = 0;
	u64 lenbit = endbit - startbit;

	bitstart = startbit%8;
	bitend = (bitstart + lenbit)%8;
	srcstart = (u8 *)src + startbit/8;
	dststart = (u8 *)dst + startbit/8;
	lenbyte =  (bitstart + lenbit)/8;
	ret = startbit;

	if (lenbyte == 0) {
		uca = *srcstart;
		ucda = *dststart;
		uca &= (1<<(8 - bitstart)) - 1;
		ucda &= (1<<(8 - bitstart)) - 1;
		uca = uca >> (8 - bitend) << (8 - bitend);
		ucda = ucda >> (8 - bitend) << (8 - bitend);
		if(uca == ucda)
			return -1;
		changebit = get_change_bit_byte(uca, ucda);
		return ret + changebit - bitstart;
	}

	if (bitstart != 0) {
		lenbyte--;
		uca = *srcstart; srcstart++;
		ucda = *dststart; dststart++;
		uca &= (1<<(8-bitstart))-1;
		ucda &= (1<<(8-bitstart))-1;
		if (uca != ucda ) {
			changebit =	get_change_bit_byte(uca, ucda);
			return ret + changebit - bitstart;
		}
		ret += 8 - bitstart;
	}

	if (((unsigned long)srcstart%8) != 0) {
		align = (unsigned long)srcstart%8;
		align = 8-align;
		if (lenbyte < align) {
			while (lenbyte >= 1) {
				uca = *srcstart;
				srcstart++;
				ucda = *dststart;
				dststart++;
				if (uca != ucda) {
					changebit = get_change_bit_byte(uca, ucda);
					return ret + changebit;
				}
				ret += 8;
				lenbyte--;
			}
		} else {

			lenbyte -= align;
			while (align >= 1) {
				uca = *srcstart;
				srcstart++;
				ucda = *dststart;
				dststart++;
				if (uca != ucda) {
					changebit = get_change_bit_byte(uca, ucda);
					return ret + changebit;
				}
				ret += 8;
				align--;
			}
		}
	}

	while (lenbyte >= 8) {
		ulla = *(u64 *)srcstart;
		ullda = *(u64*)dststart;
		if (ulla != ullda) {
			for (i = 0; i < 8; i++) {
				uca = *srcstart;
				ucda = *dststart;
				if (uca != ucda) {
					changebit = get_change_bit_byte(uca, ucda);
					return ret + changebit;
				}
				ret += 8;
				srcstart++;
				dststart++;
			}
		}
		ret += 64;
		lenbyte -= 8;
		srcstart += 8;
		dststart += 8;
	}
	while (lenbyte >= 4) {
		uia = *(u32 *)srcstart;
		uida = *(u32 *)dststart;
		if (uia != uida) {
			for (i = 0; i < 4; i++) {
				uca = *srcstart;
				ucda = *dststart;
				if (uca != ucda) {
					changebit = get_change_bit_byte(uca, ucda);
					return ret + changebit;
				}
				ret += 8;
				srcstart++;
				dststart++;
			}
		}
		ret += 32;
		lenbyte -= 4;
		srcstart += 4;
		dststart += 4;
	}
	while (lenbyte >= 2) {
		usa = *(u16 *)srcstart;
		usda = *(u16 *)dststart;
		if (usa != usda) {
			for (i = 0; i < 2; i++) {
				uca = *srcstart;
				ucda = *dststart;
				if (uca != ucda) {
					changebit = get_change_bit_byte(uca, ucda);
					return ret + changebit;
				}
				ret += 8;
				srcstart++;
				dststart++;
			}
		}
		ret += 16;
		lenbyte -= 2;
		srcstart += 2;
		dststart += 2;
	}
	while (lenbyte >= 1) {
		uca = *srcstart;
		ucda = *dststart;
		if (uca != ucda) {
			changebit = get_change_bit_byte(uca, ucda);
			return ret + changebit;
		}
		ret += 8;
		lenbyte--;
		srcstart++;
		dststart++;
	}
	if (bitend) {
		uca = *srcstart >> (8 - bitend) << (8 - bitend);
		ucda = *dststart >> (8 - bitend) << (8 - bitend);
		if(uca != ucda) {
			changebit = get_change_bit_byte(uca, ucda);
			return ret + changebit;
		}
	}
	return -1;

}
#if 0
int find_data_entry_prediction(struct cluster_head_t *pclst, struct prediction_info_t *pqinfo)
{
	int cur_data, vecid, cmp, op, cur_vecid, pre_vecid, next_vecid, cnt;
	struct spt_vec *pcur, *pnext, *ppre;
	struct spt_vec tmp_vec, cur_vec, next_vec;
	char *pcur_data;
	u64 startbit, endbit, len, fs_pos, signpost;
	int va_old, va_new;
	int ret;
	int retb;
	struct vec_cmpret_t cmpres;
	struct insert_info_t st_insert_info;
	char *pdata, *prdata;
	struct spt_dh *pdh;
	spt_cb_end_key finish_key_cb;
	int refind_cnt;
	u64 first_chbit;
	int loop_cnt = 0;
	
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
	refind_cnt = 0;

refind_start:
	pcur = pqinfo->pstart_vec;
	cur_vecid = pre_vecid = pqinfo->startid;
refind_forward:

	if (pcur == NULL)
		goto refind_start;
	ppre = NULL;
	cur_vecid = pre_vecid;
	pre_vecid = SPT_INVALID;
	cur_vec.val = pcur->val;
	if (pcur == pclst->pstart) {
		startbit = pclst->startbit;
	} else {
		startbit = signpost + get_real_pos(&cur_vec) + 1;
	}

	endbit = pqinfo->endbit;
	if (cur_vec.status == SPT_VEC_RAW) {
		smp_mb();/* ^^^ */
		cur_vec.val = pcur->val;
		if (cur_vec.status == SPT_VEC_RAW) {
			if (pcur == pqinfo->pstart_vec) {
				finish_key_cb(prdata);
				return SPT_PREDICTION_ERR;
			}
			goto refind_start;
		}
	}
	if (cur_vec.status == SPT_VEC_INVALID
		|| cur_vec.type == SPT_VEC_SIGNPOST) {
		if (pcur == pqinfo->pstart_vec) {
			finish_key_cb(prdata);
			return SPT_PREDICTION_ERR;
		}
		goto refind_start;
	}

	while (startbit < endbit) {
		loop_cnt++;
		/*first bit is 1｣ｬcompare with pcur_vec->right*/
		if (test_bit_set(prdata, startbit)) {
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

					first_chbit = get_first_change_bit(prdata,
							pcur_data,
							0,
							startbit);
					if (first_chbit == -1) {	
						pqinfo->ret_vec_id = cur_vecid; 
						pqinfo->ret_vec = pcur; 
						return SPT_PREDICTION_OK;
					}
					goto prediction_check;

				} else if (cur_data == SPT_NULL) {
					if(ppre == NULL) {
						//printf("return err line %d,loop_cnt %d,data_num %d\r\n",__LINE__, loop_cnt, total_data_num); 
						return SPT_PREDICTION_ERR;
					}
					cur_data = get_data_id(pclst, ppre);
					if (cur_data >= 0 && cur_data < SPT_INVALID) {
						pdh = (struct spt_dh *)db_id_2_ptr(pclst,
							cur_data);
						smp_mb();/* ^^^ */
						pcur_data = pclst->get_key_in_tree(pdh->pdata);
						//printf("find change bit line %d\r\n",__LINE__);
						first_chbit = get_first_change_bit(prdata,
								pcur_data,
								0,
								startbit);
						if (first_chbit == -1) {	
							pqinfo->ret_vec_id = cur_vecid; 
							pqinfo->ret_vec = pcur; 
							return SPT_PREDICTION_OK;
						}
						goto prediction_check;
					
					} else {
						//printf("return err line %d,loop_cnt %d,data_num %d\r\n",__LINE__, loop_cnt, total_data_num); 
						return SPT_PREDICTION_ERR;
					}
				}
				else {
					//printf("return err line %d\r\n",__LINE__);
					return SPT_PREDICTION_ERR;
					spt_assert(0);
				}

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
						vec_free(pclst,
								vecid);
						retb = SPT_OK;
						if (retb != SPT_OK
							&& cur_data != SPT_INVALID)
							pclst->get_key_in_tree_end(
								pcur_data);
						if (retb != SPT_OK) {
							finish_key_cb(prdata);
							return SPT_PREDICTION_ERR;;
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
				//len = next_vec.pos + signpost - startbit + 1;
				len = get_real_pos(&next_vec) + signpost - startbit + 1;

				if(startbit + len >= endbit) {
					cur_data = get_data_id(pclst, pnext);
					if (cur_data >= 0 && cur_data < SPT_INVALID) {
						pdh = (struct spt_dh *)db_id_2_ptr(pclst,
							cur_data);
						smp_mb();/* ^^^ */
						pcur_data = pclst->get_key_in_tree(pdh->pdata);
					
					} else
						return SPT_PREDICTION_ERR;

					//printf("find change bit line %d\r\n",__LINE__);
					first_chbit = get_first_change_bit(
							prdata,
							pcur_data,
							0,
							startbit);
					if(first_chbit == -1) {
						pqinfo->ret_vec_id = cur_vecid;
						pqinfo->ret_vec = pcur; 
						return SPT_PREDICTION_OK;
					}
					goto prediction_check;
				}
				if (sd_perf_debug)
					pclst->data_prediction_cnt++;
				startbit += len;
				ppre = pcur;
				pcur = pnext;
				pre_vecid = cur_vecid;
				cur_vecid = next_vecid;
				cur_vec.val = next_vec.val;
			}
		} else {

			if (cur_vec.down != SPT_NULL) {
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
						vec_free(pclst,
								vecid);
						retb = SPT_OK;
						if (retb != SPT_OK) {
							finish_key_cb(prdata);
							return SPT_PREDICTION_ERR;
						}
					}

					cur_vec.val = pcur->val;
					if (cur_vec.status != SPT_VEC_VALID) {
						pcur = ppre;
						goto refind_forward;
					}
				}

				//len = next_vec.pos + signpost - startbit + 1;
				len = get_real_pos(&next_vec) + signpost - startbit + 1;

				if (!test_bit_zero(prdata, startbit, len) 
						|| (startbit + len >= endbit)) {
					cur_data = get_data_id(pclst, pnext);
					if (cur_data >= 0 && cur_data < SPT_INVALID) {
						pdh = (struct spt_dh *)db_id_2_ptr(pclst,
							cur_data);
						smp_mb();/* ^^^ */
						pcur_data = pclst->get_key_in_tree(pdh->pdata);
					
					} else
						return SPT_PREDICTION_ERR;

					//printf("find change bit line %d\r\n",__LINE__); 
					first_chbit = get_first_change_bit(prdata,
								pcur_data,
								0,
								startbit);
					if (first_chbit == -1) {
						pqinfo->ret_vec_id = cur_vecid;
						pqinfo->ret_vec = pcur; 
						return SPT_PREDICTION_OK;
					}
					goto prediction_check;
				}

				if (sd_perf_debug)
					pclst->data_prediction_cnt++;
				startbit += len;
				ppre = pcur;
				pcur = pnext;
				pre_vecid = cur_vecid;
				cur_vecid = next_vecid;
				cur_vec.val = next_vec.val;
			 } else {
				if(ppre == NULL) {
					//printf("return err line %d,loop_cnt %d,data_num %d\r\n",__LINE__, loop_cnt, total_data_num); 
					return SPT_PREDICTION_ERR;
				}
				cur_data = get_data_id(pclst, ppre);
				if (cur_data >= 0 && cur_data < SPT_INVALID) {
					pdh = (struct spt_dh *)db_id_2_ptr(pclst,
						cur_data);
					smp_mb();/* ^^^ */
					pcur_data = pclst->get_key_in_tree(pdh->pdata);
					//printf("find change bit line %d\r\n",__LINE__);
					first_chbit = get_first_change_bit(prdata,
							pcur_data,
							0,
							startbit);
					if (first_chbit == -1) {	
						pqinfo->ret_vec_id = cur_vecid; 
						pqinfo->ret_vec = pcur; 
						return SPT_PREDICTION_OK;
					}
					goto prediction_check;
				
				} else {
					//printf("return err line %d,loop_cnt %d,data_num %d\r\n",__LINE__, loop_cnt, total_data_num); 
					return SPT_PREDICTION_ERR;
				
				}
			 }
		}
	}

prediction_check:
	return SPT_PREDICTION_ERR;
}

int find_data_prediction(struct cluster_head_t *pclst, struct prediction_info_t *pqinfo)
{
	int cur_data, vecid, cmp, op, cur_vecid, pre_vecid, next_vecid, cnt;
	struct spt_vec *pcur, *pnext, *ppre;
	struct spt_vec tmp_vec, cur_vec, next_vec;
	char *pcur_data;
	u64 startbit, endbit, len, fs_pos, signpost;
	int va_old, va_new;
	int ret;
	int retb;
	struct vec_cmpret_t cmpres;
	struct insert_info_t st_insert_info;
	char *pdata, *prdata;
	struct spt_dh *pdh;
	spt_cb_end_key finish_key_cb;
	int refind_cnt;
	u64 first_chbit;
	int loop_cnt = 0;
	
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
	refind_cnt = 0;

refind_start:
	pcur = pqinfo->pstart_vec;
	cur_vecid = pre_vecid = pqinfo->startid;
refind_forward:

	if (pcur == NULL)
		goto refind_start;
	ppre = NULL;
	cur_vecid = pre_vecid;
	pre_vecid = SPT_INVALID;
	cur_vec.val = pcur->val;
	if (pcur == pclst->pstart) {
		startbit = pclst->startbit;
	} else {
		//startbit = signpost + cur_vec.pos + 1;
		startbit = signpost + get_real_pos(&cur_vec) + 1;
	}
#if 1
	if(!refind_cnt)
		pqinfo->originbit = startbit;
	refind_cnt++;
#endif
	endbit = pqinfo->endbit;
	if (cur_vec.status == SPT_VEC_RAW) {
		smp_mb();/* ^^^ */
		cur_vec.val = pcur->val;
		if (cur_vec.status == SPT_VEC_RAW) {
			if (pcur == pqinfo->pstart_vec) {
				finish_key_cb(prdata);
				return SPT_PREDICTION_ERR;
			}
			goto refind_start;
		}
	}
	if (cur_vec.status == SPT_VEC_INVALID
		|| cur_vec.type == SPT_VEC_SIGNPOST) {
		if (pcur == pqinfo->pstart_vec) {
			finish_key_cb(prdata);
			return SPT_PREDICTION_ERR;
		}
		goto refind_start;
	}

	while (startbit < endbit) {
		loop_cnt++;
		/*first bit is 1｣ｬcompare with pcur_vec->right*/
		if (test_bit_set(prdata, startbit)) {
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

					first_chbit = get_first_change_bit(prdata,
							pcur_data,
							pqinfo->originbit,
							startbit);
					if (first_chbit == -1) {	
						pqinfo->ret_vec_id = cur_vecid; 
						pqinfo->ret_vec = pcur; 
						return SPT_PREDICTION_OK;
					}
					goto prediction_check;

				} else if (cur_data == SPT_NULL) {
					if(ppre == NULL) {
						//printf("return err line %d,loop_cnt %d,data_num %d\r\n",__LINE__, loop_cnt, total_data_num); 
						return SPT_PREDICTION_ERR;
					}
					cur_data = get_data_id(pclst, ppre);
					if (cur_data >= 0 && cur_data < SPT_INVALID) {
						pdh = (struct spt_dh *)db_id_2_ptr(pclst,
							cur_data);
						smp_mb();/* ^^^ */
						pcur_data = pclst->get_key_in_tree(pdh->pdata);
						//printf("find change bit line %d\r\n",__LINE__);
						first_chbit = get_first_change_bit(prdata,
								pcur_data,
								pqinfo->originbit,
								startbit);
						if (first_chbit == -1) {	
							pqinfo->ret_vec_id = cur_vecid; 
							pqinfo->ret_vec = pcur; 
							return SPT_PREDICTION_OK;
						}
						goto prediction_check;
					
					} else {
						//printf("return err line %d,loop_cnt %d,data_num %d\r\n",__LINE__, loop_cnt, total_data_num); 
						return SPT_PREDICTION_ERR;
					}
				}
				else {
					//printf("return err line %d\r\n",__LINE__);
					return SPT_PREDICTION_ERR;
					spt_assert(0);
				}

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
						vec_free(pclst,
								vecid);
						retb = SPT_OK;
						if (retb != SPT_OK
							&& cur_data != SPT_INVALID)
							pclst->get_key_in_tree_end(
								pcur_data);
						if (retb != SPT_OK) {
							finish_key_cb(prdata);
							return SPT_PREDICTION_ERR;;
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
				//len = next_vec.pos + signpost - startbit + 1;
				len = get_real_pos(&next_vec) + signpost - startbit + 1;

				if(startbit + len >= endbit) {
					cur_data = get_data_id(pclst, pnext);
					if (cur_data >= 0 && cur_data < SPT_INVALID) {
						pdh = (struct spt_dh *)db_id_2_ptr(pclst,
							cur_data);
						smp_mb();/* ^^^ */
						pcur_data = pclst->get_key_in_tree(pdh->pdata);
					
					} else
						return SPT_PREDICTION_ERR;

					//printf("find change bit line %d\r\n",__LINE__);
					first_chbit = get_first_change_bit(
							prdata,
							pcur_data,
							pqinfo->originbit,
							startbit);
					if(first_chbit == -1) {
						pqinfo->ret_vec_id = cur_vecid;
						pqinfo->ret_vec = pcur; 
						return SPT_PREDICTION_OK;
					}
					goto prediction_check;
				}
				if (sd_perf_debug)
					pclst->data_prediction_cnt++;
				startbit += len;
				ppre = pcur;
				pcur = pnext;
				pre_vecid = cur_vecid;
				cur_vecid = next_vecid;
				cur_vec.val = next_vec.val;
			}
		} else {

			if (cur_vec.down != SPT_NULL) {
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
						vec_free(pclst,
								vecid);
						retb = SPT_OK;
						if (retb != SPT_OK) {
							finish_key_cb(prdata);
							return SPT_PREDICTION_ERR;
						}
					}

					cur_vec.val = pcur->val;
					if (cur_vec.status != SPT_VEC_VALID) {
						pcur = ppre;
						goto refind_forward;
					}
				}

				//len = next_vec.pos + signpost - startbit + 1;
				len = get_real_pos(&next_vec) + signpost - startbit + 1;

				if (!test_bit_zero(prdata, startbit, len) 
						|| (startbit + len >= endbit)) {
					cur_data = get_data_id(pclst, pnext);
					if (cur_data >= 0 && cur_data < SPT_INVALID) {
						pdh = (struct spt_dh *)db_id_2_ptr(pclst,
							cur_data);
						smp_mb();/* ^^^ */
						pcur_data = pclst->get_key_in_tree(pdh->pdata);
					
					} else
						return SPT_PREDICTION_ERR;

					//printf("find change bit line %d\r\n",__LINE__); 
					first_chbit = get_first_change_bit(prdata,
								pcur_data,
								pqinfo->originbit,
								startbit);
					if (first_chbit == -1) {
						pqinfo->ret_vec_id = cur_vecid;
						pqinfo->ret_vec = pcur; 
						return SPT_PREDICTION_OK;
					}
					goto prediction_check;
				}

				if (sd_perf_debug)
					pclst->data_prediction_cnt++;
				startbit += len;
				ppre = pcur;
				pcur = pnext;
				pre_vecid = cur_vecid;
				cur_vecid = next_vecid;
				cur_vec.val = next_vec.val;
			 } else {
				if(ppre == NULL) {
					//printf("return err line %d,loop_cnt %d,data_num %d\r\n",__LINE__, loop_cnt, total_data_num); 
					return SPT_PREDICTION_ERR;
				}
				cur_data = get_data_id(pclst, ppre);
				if (cur_data >= 0 && cur_data < SPT_INVALID) {
					pdh = (struct spt_dh *)db_id_2_ptr(pclst,
						cur_data);
					smp_mb();/* ^^^ */
					pcur_data = pclst->get_key_in_tree(pdh->pdata);
					//printf("find change bit line %d\r\n",__LINE__);
					first_chbit = get_first_change_bit(prdata,
							pcur_data,
							pqinfo->originbit,
							startbit);
					if (first_chbit == -1) {	
						pqinfo->ret_vec_id = cur_vecid; 
						pqinfo->ret_vec = pcur; 
						return SPT_PREDICTION_OK;
					}
					goto prediction_check;
				
				} else {
					//printf("return err line %d,loop_cnt %d,data_num %d\r\n",__LINE__, loop_cnt, total_data_num); 
					return SPT_PREDICTION_ERR;
				
				}
			 }
		}
	}
prediction_check:
	pcur = pqinfo->pstart_vec;
	cur_vecid = pre_vecid = pqinfo->startid;
	if (pcur == NULL)
		goto refind_start;
	ppre = NULL;
	cur_vecid = pre_vecid;
	pre_vecid = SPT_INVALID;
	cur_vec.val = pcur->val;

	endbit = first_chbit;
	startbit = pqinfo->originbit;

	if (cur_vec.status == SPT_VEC_RAW) {
		smp_mb();/* ^^^ */
		cur_vec.val = pcur->val;
		if (cur_vec.status == SPT_VEC_RAW) {
			//printf("return err line %d\r\n",__LINE__);
			return SPT_PREDICTION_ERR;
		}
	}
	if (cur_vec.status == SPT_VEC_INVALID
		|| cur_vec.type == SPT_VEC_SIGNPOST) {
			//printf("return err line %d\r\n",__LINE__);
			return SPT_PREDICTION_ERR;
	}
	if(startbit == endbit) {
		pqinfo->ret_vec_id = cur_vecid;
		pqinfo->ret_vec = pcur; 
		return SPT_PREDICTION_OK;
	}

	while(startbit < endbit) {
		/*first bit is 1｣ｬcompare with pcur_vec->right*/
		if (test_bit_set(prdata, startbit)) {
			
			if (cur_vec.type == SPT_VEC_DATA) {
				//printf("return err line %d\r\n",__LINE__);
				return SPT_PREDICTION_ERR;
			}
			else {
				pnext = (struct spt_vec *)vec_id_2_ptr(pclst,
					cur_vec.rd);
				next_vec.val = pnext->val;
				next_vecid = cur_vec.rd;
				if (next_vec.status == SPT_VEC_RAW) {
					//printf("return err line %d\r\n",__LINE__);
					return SPT_PREDICTION_ERR;
				}
				if (next_vec.status == SPT_VEC_INVALID) {
					//printf("return err line %d\r\n",__LINE__);
					return SPT_PREDICTION_ERR;
				}

				if (next_vec.down == SPT_NULL) {
					//printf("return err line %d\r\n",__LINE__);
					return SPT_PREDICTION_ERR;
				}
				//len = next_vec.pos + signpost - startbit + 1;
				len = get_real_pos(&next_vec) + signpost - startbit + 1;
				
				if(startbit + len >= endbit) {
					pqinfo->ret_vec_id = cur_vecid;
					pqinfo->ret_vec = pcur; 
					return SPT_PREDICTION_OK;
				}
				startbit += len;
				ppre = pcur;
				pcur = pnext;
				pre_vecid = cur_vecid;
				cur_vecid = next_vecid;
				cur_vec.val = next_vec.val;
			}
		} else {

			if (cur_vec.down != SPT_NULL) {
				pnext = (struct spt_vec *)vec_id_2_ptr(pclst,
						cur_vec.down);
				next_vec.val = pnext->val;
				next_vecid = cur_vec.down;
				if (next_vec.status == SPT_VEC_RAW) {
					smp_mb();/* ^^^ */
					next_vec.val = pnext->val;
					if (next_vec.status == SPT_VEC_RAW) {
						//printf("return err line %d\r\n",__LINE__);
						return SPT_PREDICTION_ERR;
					}
				}
				if (next_vec.status == SPT_VEC_INVALID) {
					//printf("return err line %d\r\n",__LINE__);
					return SPT_PREDICTION_ERR;
				}


				//len = next_vec.pos + signpost - startbit + 1;
				len = get_real_pos(&next_vec) + signpost - startbit + 1;

				if (!test_bit_zero(prdata, startbit, len) 
						|| (startbit + len >= endbit)) {
					pqinfo->ret_vec_id = cur_vecid;
					pqinfo->ret_vec = pcur; 
					return SPT_PREDICTION_OK;
				}

				startbit += len;
				ppre = pcur;
				pcur = pnext;
				pre_vecid = cur_vecid;
				cur_vecid = next_vecid;
				cur_vec.val = next_vec.val;
			 } else {
				//printf("return err line %d\r\n",__LINE__);
				 return SPT_PREDICTION_ERR;
			 }
		}
	}
	return SPT_PREDICTION_ERR;
}


int global_entry_grp;
int debug_get_data_id = 0;
int find_data_entry(struct cluster_head_t *pclst, char *new_data, struct spt_vec **ret_vec)
{
	int entry_grp; 
	struct spt_grp *grp;
	int fs,vec_index, ret;
	struct spt_pg_h *spt_pg;
	struct spt_grp va_old, va_new;
	unsigned int allocmap, freemap;
	unsigned int next_grp;
	struct spt_vec *vec, cur_vec;
	int  cur_data;
	struct spt_dh *pdh;
	char *pcur_data;
	int first_check = 0;
	int i;
	struct prediction_info_t pre_qinfo = {0};
	pclst->data_entry++;

	*ret_vec = NULL;
	//PERF_STAT_START(grp_by_data);
	entry_grp = get_grp_by_data(pclst, new_data, 8);
	//PERF_STAT_END(grp_by_data);

	for(i = 0 ; i < 4 ; i++) {	

		//PERF_STAT_START(index_by_data);
		vec_index = get_vec_index_by_data(new_data, 8 + i);
		//PERF_STAT_END(index_by_data);
		first_check = 1;
next_grp_loop:
		pclst->data_loop++;
		if((entry_grp/GRPS_PER_PG) >= pclst->pg_num_max)
			return -1;
		global_entry_grp = entry_grp;
		spt_pg = get_vec_pg_head(pclst, entry_grp/GRPS_PER_PG); /*get page head ,if page null alloc page*/
		grp  = get_grp_from_page_head(spt_pg, entry_grp);
		va_old.val = grp->val;
		va_old.control = grp->control;
		allocmap = va_old.allocmap;
		freemap = va_old.freemap;
		allocmap = allocmap&freemap;		
		allocmap = ~allocmap;
		fs = -1;

		while (1) {
			if (first_check) {
				fs = find_next_bit(&allocmap, 32, vec_index);
				if (fs != vec_index)
					goto get_next_grp;
			} else {
#if 0
				int j = 0;
				unsigned long long vec_val;
				PERF_STAT_START(entry_vec_search);
				for (j = 0; j < 32; j++) {
					vec = (char *)grp + sizeof(struct spt_grp) + j*sizeof(struct spt_vec);
					vec_val = vec->val;
				}
				PERF_STAT_END(entry_vec_search);
#endif
				fs = find_next_bit(&allocmap, 32, fs + 1);
				if (fs >= 32)
					goto get_next_grp;
				
			}
			pclst->data_find++;
			vec = (char *)grp + sizeof(struct spt_grp) + fs*sizeof(struct spt_vec);
			cur_vec.val = vec->val;	
			if (cur_vec.status == SPT_VEC_RAW) {
				smp_mb();
				cur_vec.val = vec->val;
				if (cur_vec.status == SPT_VEC_RAW){
					if (first_check) {
						first_check = 0;
						goto get_next_grp;
					}
					continue;
				}
			}
			if (cur_vec.status == SPT_VEC_INVALID) {
				if (first_check) {
					first_check = 0;
					goto get_next_grp;
				}
				continue;
			}
			if ((get_real_pos(&cur_vec) + 1 != 8 + i)) {
				if (first_check) {
					first_check = 0;
					goto get_next_grp;
				}
				continue;
			}
			debug_get_data_id = 1;
			*ret_vec = vec;
			return entry_grp*VEC_PER_GRP + fs;
get_next_grp:
			
			next_grp = grp->next_grp;
			if (next_grp == 0 || next_grp == 0xFFFFF)
				return -1;
			entry_grp = next_grp;
			goto next_grp_loop;

		}
	}
	return -1;
}
#endif
