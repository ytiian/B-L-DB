#include <stdint.h>
#include "db/run_manager.h"
#include "gtest/gtest.h"
#include <iostream>

namespace leveldb{

TEST(SortedRunBasic, Search)
{
    RunManager manager;
    SortedRun* run1 = manager.NewRun(1);//第1层生成一个run1：包含11，12，13，14
    //由1，2，3，4compaction生成run1
    uint64_t number = run1->GetID();
    manager.InsertFileToRun(1, run1);
    manager.InsertFileToRun(2, run1);
    manager.InsertFileToRun(3, run1);
    manager.InsertFileToRun(4, run1);
    run1->InsertContainFile(11);
    run1->InsertContainFile(12);
    run1->InsertContainFile(13);
    run1->InsertContainFile(14);

    SortedRun* run2 = manager.NewRun(1);//第1层生成一个run1：包含11，12，13，14

    //由1，2，3，4compaction生成run1
    number = run2->GetID();
    manager.InsertFileToRun(5, run2);
    manager.InsertFileToRun(6, run2);
    manager.InsertFileToRun(7, run2);
    manager.InsertFileToRun(8, run2);

    run2->InsertContainFile(21);
    run2->InsertContainFile(22);
    run2->InsertContainFile(23);
    run2->InsertContainFile(24);
    //run1和run2生成run3【31，32，33，34】
    SortedRun* run3 = manager.NewRun(2);
    run3->InsertContainFile(31);
    run3->InsertContainFile(32);
    run3->InsertContainFile(33);
    run3->InsertContainFile(34);
    run1->UpdateNext(run3);
    run2->UpdateNext(run3);
    SortedRun* final_run = nullptr;
    bool result = manager.FindFinalRun(10, &final_run);
    EXPECT_EQ(false, result);//10还在L0

    SortedRun* final = nullptr;
    result = manager.FindFinalRun(1, &final);
    EXPECT_EQ(true, result);
    EXPECT_EQ(3, final->GetID());
}


}