package com.nowcoder.community.service;

import com.nowcoder.community.dao.elasticsearch.DiscussPostRepository;
import com.nowcoder.community.entity.DiscussPost;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class ElasticsearchService {

    @Autowired
    private DiscussPostRepository discussPostRepository;

    @Autowired
    private ElasticsearchRestTemplate elasticsearchRestTemplate;

    public void saveDiscussPost(DiscussPost post) {
        discussPostRepository.save(post);
    }

    public void deleteDiscussPost(int id) {
        discussPostRepository.deleteById(id);
    }

    public Map<String, Object> searchDiscussPost(String keyword, int current, int limit) {
        NativeSearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(QueryBuilders.multiMatchQuery(keyword,"title", "content"))
                .withSort(SortBuilders.fieldSort("type").order(SortOrder.DESC))
                .withSort(SortBuilders.fieldSort("score").order(SortOrder.DESC))
                .withSort(SortBuilders.fieldSort("createTime").order(SortOrder.DESC))
                .withPageable(PageRequest.of(current, limit))
                .withHighlightFields(
                        new HighlightBuilder.Field("title").preTags("<em>").postTags("</em>"),
                        new HighlightBuilder.Field("content").preTags("<em>").postTags("</em>")
                ).build();

        SearchHits<DiscussPost> searchHits = elasticsearchRestTemplate.search(searchQuery, DiscussPost.class);
        System.out.println(searchHits.getTotalHits());
        if (searchHits.getTotalHits() <= 0) {
            return null;
        }
        List<DiscussPost> list = new ArrayList<>();
        for (SearchHit<DiscussPost> hit : searchHits) {
            DiscussPost post = new DiscussPost();
            int id = hit.getContent().getId();
            post.setId(id);

            int userId = hit.getContent().getUserId();
            post.setUserId(userId);

            String title = hit.getContent().getTitle();
            post.setTitle(title);

            String content = hit.getContent().getContent();
            post.setContent(content);

            int status = hit.getContent().getStatus();
            post.setStatus(status);

            Date date = hit.getContent().getCreateTime();
            post.setCreateTime(date);

            int commentCount = hit.getContent().getCommentCount();
            post.setCommentCount(commentCount);

            // 处于高亮显示结果
            List<String> titleField = hit.getHighlightFields().get("title");
            if (titleField != null) {
                for (String s : titleField) {
                    System.out.println(s);
                }
                post.setTitle(titleField.get(0));
            }
            List<String> contentField = hit.getHighlightFields().get("content");
            if (contentField != null) {
                for (String s : contentField) {
                    System.out.println(s);
                }
                post.setContent(contentField.get(0));
            }

            list.add(post);
        }
        for (DiscussPost post : list) {
            System.out.println(post);
        }
        Map<String, Object> map = new HashMap<>();
        map.put("list", list);
        map.put("total", searchHits.getTotalHits());
        return map;
    }

}
