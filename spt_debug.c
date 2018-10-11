/*
This file is provided under a dual BSD/GPLv2 license. When using or
redistributing this file, you may do so under either license.

GPL LICENSE SUMMARY
Copyright(c) 2016 Baibantech Corporation.
This program is free software; you can redistribute it and/or modify
it under the terms of version 2 of the GNU General Public License as
published by the Free Software Foundation.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
General Public License for more details.

BSD LICENSE
Copyright(c) 2016 Baibantech Corporation.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

* Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in
the documentation and/or other materials provided with the
distribution.
* Neither the name of Intel Corporation nor the names of its
contributors may be used to endorse or promote products derived
from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

ONE(Object Non-duplicate Engine) thread management

Contact Information:
info-linux <info@baibantech.com.cn>
*/
#include "chunk.h"
#include "hash_strategy.h"
int spt_vec_debug_info_init(struct cluster_head_t *pclst)
{
	int i;
	int entry_num; 
	struct spt_vec_debug *debug;
	entry_num = (DATA_SIZE / HASH_WINDOW_LEN) + 1;
	debug = spt_malloc(entry_num * sizeof(struct spt_vec_debug));
	if (debug) {
		memset(debug , 0, entry_num *sizeof(struct spt_vec_debug));
		for (i = 0; i< entry_num; i++)
			debug[i].window_id = i;	
		pclst->vec_debug = debug;
		return 0;
	}
	spt_assert(0);
	return -1;
}

void add_debug_cnt(struct cluster_head_t *pclst, int real_pos, int flag)
{
	int cur_window = (real_pos / 8)/HASH_WINDOW_LEN;
	if (flag == SPT_VEC_PVALUE)
		atomic_add(1, (atomic_t *)&(pclst->vec_debug[cur_window].pre_vec_cnt));
	else
		atomic_add(1, (atomic_t *)&(pclst->vec_debug[cur_window].hash_vec_cnt));
}

void sub_debug_cnt(struct cluster_head_t *pclst, int real_pos, int flag)
{
	int cur_window = (real_pos / 8)/HASH_WINDOW_LEN;
	if (flag == SPT_VEC_PVALUE)
		atomic_sub(1, (atomic_t *)&(pclst->vec_debug[cur_window].pre_vec_cnt));
	else
		atomic_sub(1, (atomic_t *)&(pclst->vec_debug[cur_window].hash_vec_cnt));
}
