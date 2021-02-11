/*
 * Copyright (c) 2020 - 2021.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jindb for more information about this project and its license.
 */

use jin::plan::insert::InsertPlanNode;
use jin::plan::QueryPlanNode;
use jin::relation::Schema;
use std::sync::Arc;

/// Tests for query execution.
/// A query plan is a tree structure constructed out of plan nodes. During execution, the query
/// plan tree is traversed and an executor is constructed at every plan node.
/// Executors are responsible for producing records for the caller according to the "Volcano Model".

/// Return the following query plan:
///
///
///
/// This plan reflects the SQL statement: "SELECT id, name from STUDENTS;"
fn setup() -> Box<dyn QueryPlanNode> {
    let query_plan = InsertPlanNode::new(1, Arc::new(Schema::new(vec![])));
    Box::new(query_plan)
}

#[ignore]
#[test]
fn test_execute_query_plan() {
    let _root = setup();
    assert!(false);
}
