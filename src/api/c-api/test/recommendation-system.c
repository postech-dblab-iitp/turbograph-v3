#include "api/c-api/s62.h"
#include <stdio.h>
#include <stdlib.h>

void recommend_friends(int userId) {
    s62_prepared_statement *stmt = s62_prepare(
        "MATCH (p:Person {id: $userId})-[:KNOWS]->(friend)-[:KNOWS]->(recommended) "
        "WHERE NOT p = recommended AND NOT EXISTS {MATCH (p)-[:KNOWS]->(recommended)} "
        "RETURN recommended.id AS recommendedFriend, COUNT(friend) AS mutualFriends "
        "ORDER BY mutualFriends DESC LIMIT 10;"
    );
    s62_bind_int64(stmt, 1, userId);

    s62_resultset_wrapper *resultset;
    s62_execute(stmt, &resultset);

    printf("Friend Recommendations for User %d:\n", userId);
    while (s62_fetch_next(resultset) != S62_END_OF_RESULT) {
        int64_t recommendedFriend = s62_get_int64(resultset, 0);
        int64_t mutualFriends = s62_get_int64(resultset, 1);
        printf("Recommended Friend ID: %d, Mutual Friends: %d\n", recommendedFriend, mutualFriends);
    }

    s62_close_resultset(resultset);
    s62_close_prepared_statement(stmt);
}

void recommend_posts(int userId) {
    s62_prepared_statement *stmt = s62_prepare(
        "MATCH (p:Person {id: $userId})-[:KNOWS]->(friend) "
        "MATCH (friend)-[:LIKES_POST]->(post:Post) "
        "WHERE NOT EXISTS {MATCH (p)-[:LIKES_POST]->(post)} "
        "RETURN post.id AS recommendedPost, COUNT(friend) AS friendLikes "
        "ORDER BY friendLikes DESC LIMIT 10;"
    );
    s62_bind_int64(stmt, 1, userId);

    s62_resultset_wrapper *resultset;
    s62_execute(stmt, &resultset);

    printf("Content Recommendations for User %d:\n", userId);
    while (s62_fetch_next(resultset) != S62_END_OF_RESULT) {
        int64_t recommendedPost = s62_get_int64(resultset, 0);
        int64_t friendLikes = s62_get_int64(resultset, 1);
        printf("Recommended Post ID: %d, Friend Likes: %d\n", recommendedPost, friendLikes);
    }

    s62_close_resultset(resultset);
    s62_close_prepared_statement(stmt);
}

void recommend_forums(int userId) {
    s62_prepared_statement *stmt = s62_prepare(
        "MATCH (p:Person {id: $userId})<-[:POST_HAS_CREATOR]-(content)-[:POST_HAS_TAG]->(tag) "
        "MATCH (forum:Forum)-[:FORUM_HAS_TAG]->(tag) "
        "WHERE NOT EXISTS {MATCH (p)<-[:POST_HAS_CREATOR]-(:Post)-[:POST_HAS_TAG]->(tag)<-[:FORUM_HAS_TAG]-(forum)} "
        "RETURN forum.id AS recommendedForum, COUNT(DISTINCT tag) AS sharedTags "
        "ORDER BY sharedTags DESC LIMIT 5;"
    );
    s62_bind_int64(stmt, 1, userId);

    s62_resultset_wrapper *resultset;
    s62_execute(stmt, &resultset);

    printf("Forum Recommendations for User %d:\n", userId);
    while (s62_fetch_next(resultset) != S62_END_OF_RESULT) {
        int64_t recommendedForum = s62_get_int64(resultset, 0);
        int64_t sharedTags = s62_get_int64(resultset, 1);
        printf("Recommended Forum ID: %d, Shared Tags: %d\n", recommendedForum, sharedTags);
    }

    s62_close_resultset(resultset);
    s62_close_prepared_statement(stmt);
}

void recommend_friends_by_location(int userId) {
    s62_prepared_statement *stmt = s62_prepare(
        "MATCH (p:Person {id: $userId})-[:IS_LOCATED_IN]->(location) "
        "MATCH (recommended:Person)-[:IS_LOCATED_IN]->(location) "
        "WHERE NOT p = recommended AND NOT EXISTS {MATCH (p)-[:KNOWS]->(recommended)} "
        "RETURN recommended.id AS recommendedFriend, location.name AS sharedLocation "
        "LIMIT 10;"
    );
    s62_bind_int64(stmt, 1, userId);

    s62_resultset_wrapper *resultset;
    s62_execute(stmt, &resultset);

    printf("Location-Based Friend Recommendations for User %d:\n", userId);
    while (s62_fetch_next(resultset) != S62_END_OF_RESULT) {
        int64_t recommendedFriend = s62_get_int64(resultset, 0);
        s62_string sharedLocation = s62_get_varchar(resultset, 1);
        printf("Recommended Friend ID: %d, Shared Location: %s\n", recommendedFriend, sharedLocation.data);
    }

    s62_close_resultset(resultset);
    s62_close_prepared_statement(stmt);
}

int main() {
    char data_folder[256];
    printf("Enter data folder path: ");
    scanf("%s", data_folder);

    s62_state state = s62_connect(data_folder);
    printf("s62_connect() done\n");
    printf("state: %d\n", state);

    int choice, userId;
    while (1) {
        printf("\nSelect Recommendation Type:\n");
        printf("1. Friend Recommendation\n");
        printf("2. Content Recommendation\n");
        printf("3. Forum Recommendation\n");
        printf("4. Location-Based Friend Recommendation\n");
        printf("5. Exit\n");
        printf("Choice: ");
        scanf("%d", &choice);

        if (choice == 5) break;

        printf("Enter User ID: ");
        scanf("%d", &userId);

        switch (choice) {
            case 1:
                recommend_friends(userId);
                break;
            case 2:
                recommend_posts(userId);
                break;
            case 3:
                recommend_forums(userId);
                break;
            case 4:
                recommend_friends_by_location(userId);
                break;
            default:
                printf("Invalid choice. Please try again.\n");
        }
    }

    s62_disconnect();
    return 0;
}
