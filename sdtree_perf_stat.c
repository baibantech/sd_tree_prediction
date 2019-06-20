#include <sdtree_perf_stat.h>


PERF_STAT_DEFINE(whole_insert);
PERF_STAT_DEFINE(insert_up_rd);
PERF_STAT_DEFINE(insert_down_rd);
PERF_STAT_DEFINE(insert_last_down);
PERF_STAT_DEFINE(insert_up_down);
PERF_STAT_DEFINE(insert_first_set);
PERF_STAT_DEFINE(whole_delete);
PERF_STAT_DEFINE(whole_query_by_hash);
PERF_STAT_DEFINE(leaf_data_prediction_vec);
PERF_STAT_DEFINE(leaf_data_check_vec);
PERF_STAT_DEFINE(calc_hash_start_vec);
PERF_STAT_DEFINE(spt_cluster_scan_perf);
PERF_STAT_DEFINE(rbtree_scan_perf);
PERF_STAT_DEFINE(find_vec_from_module);
PERF_STAT_DEFINE(find_vec_from_module1);
PERF_STAT_DEFINE(find_stable_tree);
PERF_STAT_DEFINE(scan_grp_vec);
PERF_STAT_DEFINE(module_tree_get_data);
PERF_STAT_DEFINE(test_delete_data);
PERF_STAT_DEFINE(final_find_data);





sd_perf_stat *sd_perf_stat_array[] =
{
    PERF_STAT_PTR(whole_insert),
	PERF_STAT_PTR(insert_up_rd),
	PERF_STAT_PTR(insert_down_rd),
	PERF_STAT_PTR(insert_last_down),
	PERF_STAT_PTR(insert_up_down),
	PERF_STAT_PTR(insert_first_set),
    PERF_STAT_PTR(whole_delete),
    PERF_STAT_PTR(whole_query_by_hash),
    PERF_STAT_PTR(leaf_data_prediction_vec),
    PERF_STAT_PTR(leaf_data_check_vec),
    PERF_STAT_PTR(calc_hash_start_vec),
    PERF_STAT_PTR(spt_cluster_scan_perf),
    PERF_STAT_PTR(rbtree_scan_perf),
    PERF_STAT_PTR(find_vec_from_module),
    PERF_STAT_PTR(find_vec_from_module1),
    PERF_STAT_PTR(find_stable_tree),
    PERF_STAT_PTR(scan_grp_vec),
    PERF_STAT_PTR(module_tree_get_data),
    PERF_STAT_PTR(test_delete_data),
    PERF_STAT_PTR(final_find_data),
};

void sd_perf_stat_init()
{
    int i,j;
    for(i=0;i<sizeof(sd_perf_stat_array)/sizeof(sd_perf_stat *); i++)
    {
        for(j=0;j<THREAD_NUM;j++)
            sd_perf_stat_array[i]->min[j] = 0xffffffffffffffff;
    }
}

void show_sd_perf_stat_all(void)
{
	int i,j;
	int num = sizeof(sd_perf_stat_array)/sizeof(sd_perf_stat *);
	sd_perf_stat * ptr;
	for (i = 0; i < num; i++)
	{
		ptr = sd_perf_stat_array[i];

        for(j=0;j < THREAD_NUM; j++)
        {
            if(ptr->cnt[j] == 0)
                continue;
            printf("\r\n-----------------%s\t%d----------------------\r\n",ptr->name, j);
            printf("total cycle: %lld\r\n", ptr->total[j]);
            printf("cnt        : %lld\r\n", ptr->cnt[j]);
            printf("max        : %lld\r\n", ptr->max[j]);
            printf("min        : %lld\r\n", ptr->min[j]);
            printf("av        : %lld\r\n", ptr->total[j]/ptr->cnt[j]);
        }
	}
}

void show_sd_perf_stat_thread(int id)
{
	int i,j;
	int num = sizeof(sd_perf_stat_array)/sizeof(sd_perf_stat *);
	sd_perf_stat *ptr;
	for (i = 0; i < num; i++)
	{
		ptr = sd_perf_stat_array[i];
        printf("\r\n-----------------%s\t%d----------------------\r\n",ptr->name, id);
        printf("total cycle: %lld\r\n", ptr->total[id]);
        printf("cnt        : %lld\r\n", ptr->cnt[id]);
        printf("max        : %lld\r\n", ptr->max[id]);
        printf("min        : %lld\r\n", ptr->min[id]);
        printf("av        : %lld\r\n", ptr->total[id]/ptr->cnt[id]);
        ptr->total[id] = 0;
        ptr->cnt[id] = 0;
        ptr->max[id] = 0;
        ptr->min[id] = 0xffffffffffffffff;
	}
}



