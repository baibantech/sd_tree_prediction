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
