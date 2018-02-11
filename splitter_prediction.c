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

	if (pclst->status == SPT_WAIT_AMT) {
		cnt = rsv_list_fill_cnt(pclst, g_thrd_id);
		ret = fill_in_rsv_list(pclst, cnt, g_thrd_id);
		if (ret == SPT_OK)
			pclst->status = SPT_OK;
		else
			return SPT_PREDICTION_ERR;
	}
	
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
		startbit = signpost + cur_vec.pos + 1;
	}
	if(!refind_cnt)
		pqinfo->originbit = startbit;
	refind_cnt++;

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

					printf("find change bit line %d\r\n",__LINE__);
					first_chbit = get_first_change_bit(prdata,
							pcur_data,
							pqinfo->originbit,
							startbit);
					if (first_chbit == -1) {	
						pqinfo->ret_vec_id = cur_vecid; 
						return SPT_PREDICTION_OK;
					}
					goto prediction_check;

				} else if (cur_data == SPT_NULL) {
					if(ppre == NULL) {
						printf("return err line %d\r\n",__LINE__); 
						return SPT_PREDICTION_ERR;
					}
					cur_data = get_data_id(pclst, ppre);
					if (cur_data >= 0 && cur_data < SPT_INVALID) {
						pdh = (struct spt_dh *)db_id_2_ptr(pclst,
							cur_data);
						smp_mb();/* ^^^ */
						pcur_data = pclst->get_key_in_tree(pdh->pdata);
					
					} else
						return SPT_PREDICTION_ERR;

					printf("return err line %d\r\n",__LINE__); 
					return SPT_PREDICTION_ERR; 
				}
				else {
					printf("return err line %d\r\n",__LINE__);
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
						retb = vec_free_to_buf(pclst,
								vecid,
								g_thrd_id);
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
				len = next_vec.pos + signpost - startbit + 1;

				if(startbit + len >= endbit) {
					cur_data = get_data_id(pclst, pnext);
					if (cur_data >= 0 && cur_data < SPT_INVALID) {
						pdh = (struct spt_dh *)db_id_2_ptr(pclst,
							cur_data);
						smp_mb();/* ^^^ */
						pcur_data = pclst->get_key_in_tree(pdh->pdata);
					
					} else
						return SPT_PREDICTION_ERR;

					printf("find change bit line %d\r\n",__LINE__);
					first_chbit = get_first_change_bit(
							prdata,
							pcur_data,
							pqinfo->originbit,
							startbit);
					if(first_chbit == -1) {
						pqinfo->ret_vec_id = cur_vecid;
						return SPT_PREDICTION_OK;
					}
					goto prediction_check;
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
						retb = vec_free_to_buf(pclst,
								vecid, g_thrd_id);
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

				len = next_vec.pos + signpost - startbit + 1;

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

					printf("find change bit line %d\r\n",__LINE__); 
					first_chbit = get_first_change_bit(prdata,
								pcur_data,
								pqinfo->originbit,
								startbit);
					if (first_chbit == -1) {
						pqinfo->ret_vec_id = cur_vecid;
						return SPT_PREDICTION_OK;
					}
					goto prediction_check;
				}

				startbit += len;
				ppre = pcur;
				pcur = pnext;
				pre_vecid = cur_vecid;
				cur_vecid = next_vecid;
				cur_vec.val = next_vec.val;
			 } else {
				printf("return err line %d\r\n",__LINE__);
				return SPT_PREDICTION_ERR;
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
			printf("return err line %d\r\n",__LINE__);
			return SPT_PREDICTION_ERR;
		}
	}
	if (cur_vec.status == SPT_VEC_INVALID
		|| cur_vec.type == SPT_VEC_SIGNPOST) {
			printf("return err line %d\r\n",__LINE__);
			return SPT_PREDICTION_ERR;
	}
	if(startbit == endbit) {
		pqinfo->ret_vec_id = cur_vecid;
		return SPT_PREDICTION_OK;
	}

	while(startbit < endbit) {
		/*first bit is 1｣ｬcompare with pcur_vec->right*/
		if (test_bit_set(prdata, startbit)) {
			
			if (cur_vec.type == SPT_VEC_DATA) {
				printf("return err line %d\r\n",__LINE__);
				return SPT_PREDICTION_ERR;
			}
			else {
				pnext = (struct spt_vec *)vec_id_2_ptr(pclst,
					cur_vec.rd);
				next_vec.val = pnext->val;
				next_vecid = cur_vec.rd;
				if (next_vec.status == SPT_VEC_RAW) {
					printf("return err line %d\r\n",__LINE__);
					return SPT_PREDICTION_ERR;
				}
				if (next_vec.status == SPT_VEC_INVALID) {
					printf("return err line %d\r\n",__LINE__);
					return SPT_PREDICTION_ERR;
				}

				if (next_vec.down == SPT_NULL) {
					printf("return err line %d\r\n",__LINE__);
					return SPT_PREDICTION_ERR;
				}
				len = next_vec.pos + signpost - startbit + 1;
				if(startbit + len >= endbit) {
					pqinfo->ret_vec_id = cur_vecid;
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
						printf("return err line %d\r\n",__LINE__);
						return SPT_PREDICTION_ERR;
					}
				}
				if (next_vec.status == SPT_VEC_INVALID) {
					printf("return err line %d\r\n",__LINE__);
					return SPT_PREDICTION_ERR;
				}


				len = next_vec.pos + signpost - startbit + 1;

				if (!test_bit_zero(prdata, startbit, len) 
						|| (startbit + len >= endbit)) {
					pqinfo->ret_vec_id = cur_vecid;
					return SPT_PREDICTION_OK;
				}

				startbit += len;
				ppre = pcur;
				pcur = pnext;
				pre_vecid = cur_vecid;
				cur_vecid = next_vecid;
				cur_vec.val = next_vec.val;
			 } else {
				printf("return err line %d\r\n",__LINE__);
				 return SPT_PREDICTION_ERR;
			 }
		}
	}
	return SPT_PREDICTION_ERR;
}
