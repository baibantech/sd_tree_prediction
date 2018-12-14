#include "rbtree_adp.h"

struct rb_root data_rb_root = RB_ROOT;

char *data_rb_tree_insert(char *data)
{
	struct data_rb_tree_node *node = NULL;
	struct rb_node **new;
	struct rb_node *parent = NULL;
	char *tree_data;
	int ret;
	new = &(data_rb_root.rb_node);
	while (*new) {
		node = rb_entry(*new, struct data_rb_tree_node, rb_node);
		tree_data = node->data;
		if (!tree_data)
			return NULL;
		ret = memcmp(data, tree_data, 256);
		
		parent = *new;
		if (ret < 0)
			new = &parent->rb_left;
		else if (ret > 0)
			new = &parent->rb_right;
		else {
			node->ref++;
			return node->data;
		}
	}
	node = spt_malloc(sizeof(struct data_rb_tree_node));
	node->data = data;
	node->ref = 1;
	rb_link_node(&node->rb_node, parent, new);
	rb_insert_color(&node->rb_node, &data_rb_root);	
	return data;
}

char *data_rb_tree_find(char *data)
{
	struct data_rb_tree_node *node = NULL;
	struct rb_node **new;
	struct rb_node *parent = NULL;
	char *tree_data;
	int ret;
	new = &(data_rb_root.rb_node);
	while (*new) {
		node = rb_entry(*new, struct data_rb_tree_node, rb_node);
		tree_data = node->data;
		if (!tree_data)
			return NULL;
		ret = memcmp(data, tree_data, 256);
		
		parent = *new;
		if (ret < 0)
			new = &parent->rb_left;
		else if (ret > 0)
			new = &parent->rb_right;
		else {
			node->ref++;
			return node->data;
		}
	}
	return NULL;
}
