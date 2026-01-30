// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#pragma once
#include <limits>
#include "inc/Core/Common.h"
#include "inc/Core/Common/DistanceUtils.h"
#include "inc/Core/Common/QueryResultSet.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Helper/VectorSetReader.h"
#include "inc/Helper/StringConvert.h"
#include "inc/SSDServing/Utils.h"

namespace SPTAG {
	namespace SSDServing {
		namespace SSDIndex {

            template <typename ValueType>
            ErrorCode OutputResult(const std::string& p_output, std::vector<QueryResult>& p_results, int p_resultNum)
            {
                if (!p_output.empty())
                {
                    auto ptr = f_createIO();
                    if (ptr == nullptr || !ptr->Initialize(p_output.c_str(), std::ios::binary | std::ios::out)) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed create file: %s\n", p_output.c_str());
                        return ErrorCode::FailedCreateFile;
                    }
                    int32_t i32Val = static_cast<int32_t>(p_results.size());
                    if (ptr->WriteBinary(sizeof(i32Val), reinterpret_cast<char*>(&i32Val)) != sizeof(i32Val)) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to write result file!\n");
                        return ErrorCode::DiskIOFail;
                    }
                    i32Val = p_resultNum;
                    if (ptr->WriteBinary(sizeof(i32Val), reinterpret_cast<char*>(&i32Val)) != sizeof(i32Val)) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to write result file!\n");
                        return ErrorCode::DiskIOFail;
                    }

                    float fVal = 0;
                    for (size_t i = 0; i < p_results.size(); ++i)
                    {
                        for (int j = 0; j < p_resultNum; ++j)
                        {
                            i32Val = p_results[i].GetResult(j)->VID;
                            if (ptr->WriteBinary(sizeof(i32Val), reinterpret_cast<char*>(&i32Val)) != sizeof(i32Val)) {
                                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to write result file!\n");
                                return ErrorCode::DiskIOFail;
                            }

                            fVal = p_results[i].GetResult(j)->Dist;
                            if (ptr->WriteBinary(sizeof(fVal), reinterpret_cast<char*>(&fVal)) != sizeof(fVal)) {
                                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to write result file!\n");
                                return ErrorCode::DiskIOFail;
                            }
                        }
                    }
                }
                return ErrorCode::Success;
            }

            template<typename T, typename V>
            void PrintPercentiles(const std::vector<V>& p_values, std::function<T(const V&)> p_get, const char* p_format)
            {
                double sum = 0;
                std::vector<T> collects;
                collects.reserve(p_values.size());
                for (const auto& v : p_values)
                {
                    T tmp = p_get(v);
                    sum += tmp;
                    collects.push_back(tmp);
                }

                std::sort(collects.begin(), collects.end());

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Avg\t50tiles\t90tiles\t95tiles\t99tiles\t99.9tiles\tMax\n");

                std::string formatStr("%.3lf");
                for (int i = 1; i < 7; ++i)
                {
                    formatStr += '\t';
                    formatStr += p_format;
                }

                formatStr += '\n';

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                    formatStr.c_str(),
                    sum / collects.size(),
                    collects[static_cast<size_t>(collects.size() * 0.50)],
                    collects[static_cast<size_t>(collects.size() * 0.90)],
                    collects[static_cast<size_t>(collects.size() * 0.95)],
                    collects[static_cast<size_t>(collects.size() * 0.99)],
                    collects[static_cast<size_t>(collects.size() * 0.999)],
                    collects[static_cast<size_t>(collects.size() - 1)]);
            }


            template <typename ValueType>
            void SearchSequential(SPANN::Index<ValueType>* p_index,
                int p_numThreads,
                std::vector<QueryResult>& p_results,
                std::vector<SPANN::SearchStats>& p_stats,
                int p_maxQueryCount, int p_internalResultNum)
            {//线程数，存储搜索结果，存储搜索统计信息
                int numQueries = min(static_cast<int>(p_results.size()), p_maxQueryCount);//外部准备好的query数量，最多允许处理的query数量

                std::atomic_size_t queriesSent(0);//原子计数器，动态分发query

                std::vector<std::thread> threads;
                threads.reserve(p_numThreads);
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Searching: numThread: %d, numQueries: %d.\n", p_numThreads, numQueries);

                Utils::StopW sw;

                for (int i = 0; i < p_numThreads; i++) { threads.emplace_back([&, i]()
                    {//开启lambda表达式线程函数：定义每个线程要执行的具体任务
                        NumaStrategy ns = (p_index->GetDiskIndex() != nullptr) ? NumaStrategy::SCATTER : NumaStrategy::LOCAL; // Only for SPANN, we need to avoid IO threads overlap with search threads.
                        Helper::SetThreadAffinity(i, threads[i], ns, OrderStrategy::ASC); //决定NUMA策略：如果有磁盘索引，使用SCATTER打散，避免io线程与计算线程冲突，设置线程亲和性，将线程绑定到特定的cpu核心来减少上下文切换

                        Utils::StopW threadws;
                        size_t index = 0;
                        while (true)
                        {
                            index = queriesSent.fetch_add(1);//获取当前任务索引，并将计数器加一
                            if (index < numQueries)//如果还有查询没有做完
                            {
                                if ((index & ((1 << 14) - 1)) == 0)//每发送2的14次方个查询，打印一次进度日志
                                {
                                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Sent %.2lf%%...\n", index * 100.0 / numQueries);
                                }

                                double startTime = threadws.getElapsedMs();//内存索引搜索
                                p_index->GetMemoryIndex()->SearchIndex(p_results[index]);//在内存中寻找与当前query最近的head 候选点
                                double endTime = threadws.getElapsedMs();
                                p_index->SearchDiskIndex(p_results[index], &(p_stats[index]));//根据上面找到的中心点，去磁盘上读取对应的postinglist,进行最终的向量距离比对和TopK排序
                                double exEndTime = threadws.getElapsedMs();

                                p_stats[index].m_exLatency = exEndTime - endTime;//记录磁盘搜索耗时
                                p_stats[index].m_totalLatency = p_stats[index].m_totalSearchLatency = exEndTime - startTime;//记录内存+磁盘搜索的总耗时
                            }
                            else
                            {
                                return;
                            }
                        }
                    });
                }
                for (auto& thread : threads) { thread.join(); }

                double sendingCost = sw.getElapsedSec();//计算并记录所有查询完成处理所花费的总时间。

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                    "Finish sending in %.3lf seconds, actuallQPS is %.2lf, query count %u.\n",
                    sendingCost,
                    numQueries / sendingCost,
                    static_cast<uint32_t>(numQueries));//QPS=查询量/总时间

                for (int i = 0; i < numQueries; i++) { p_results[i].CleanQuantizedTarget(); }//释放内存
            }

            template <typename ValueType>
            ErrorCode Search(SPANN::Index<ValueType>* p_index)
            {
                SPANN::Options& p_opts = *(p_index->GetOptions());
                std::string outputFile = p_opts.m_searchResult;//实际搜索结果输出
                std::string truthFile = p_opts.m_truthPath;
                std::string warmupFile = p_opts.m_warmupPath;

                if (p_index->m_pQuantizer)
                {
                   p_index->m_pQuantizer->SetEnableADC(p_opts.m_enableADC);
                }

                if (!p_opts.m_logFile.empty())
                {
                    SetLogger(std::make_shared<Helper::FileLogger>(Helper::LogLevel::LL_Info, p_opts.m_logFile.c_str()));
                }
                int numThreads = p_opts.m_searchThreadNum;
                int internalResultNum = p_opts.m_searchInternalResultNum;//SSD+Head搜索时，候选结果数量
                int K = p_opts.m_resultNum;
                int truthK = (p_opts.m_truthResultNum <= 0) ? K : p_opts.m_truthResultNum;
                ErrorCode ret;

                if (!warmupFile.empty())
                {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Start loading warmup query set...\n");
                    std::shared_ptr<Helper::ReaderOptions> queryOptions(new Helper::ReaderOptions(p_opts.m_valueType, p_opts.m_dim, p_opts.m_warmupType, p_opts.m_warmupDelimiter));
                    auto queryReader = Helper::VectorSetReader::CreateInstance(queryOptions);//创建读取器实例
                    if (ErrorCode::Success != (ret = queryReader->LoadFile(p_opts.m_warmupPath)))
                    {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read query file.\n");
                        return ret;
                    }
                    auto warmupQuerySet = queryReader->GetVectorSet();//获取加载好的向量集合
                    int warmupNumQueries = warmupQuerySet->Count();//获取预热查询的数量

                    std::vector<QueryResult> warmupResults(warmupNumQueries, QueryResult(NULL, max(K, internalResultNum), false));//为每个预热查询分配结果空间
                    std::vector<SPANN::SearchStats> warmpUpStats(warmupNumQueries);//用于记录搜索过程中的统计数据
                    for (int i = 0; i < warmupNumQueries; ++i)
                    {
                        (*((COMMON::QueryResultSet<ValueType>*)&warmupResults[i])).SetTarget(reinterpret_cast<ValueType*>(warmupQuerySet->GetVector(i)), p_index->m_pQuantizer);//将预热集中的第i个向量作为搜索目标
                        warmupResults[i].Reset();//重置结果集状态
                    }

                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Start warmup...\n");
                    SearchSequential(p_index, numThreads, warmupResults, warmpUpStats, p_opts.m_queryCountLimit, internalResultNum);//预热搜索，会产生真实的io，操作系统将索引加载到缓冲中。
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "\nFinish warmup...\n");
                }

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Start loading QuerySet...\n");
                std::shared_ptr<Helper::ReaderOptions> queryOptions(new Helper::ReaderOptions(p_opts.m_valueType, p_opts.m_dim, p_opts.m_queryType, p_opts.m_queryDelimiter));//创建一个读取器配置对象
                auto queryReader = Helper::VectorSetReader::CreateInstance(queryOptions);//根据配置实例化一个向量读取器
                if (ErrorCode::Success != (ret = queryReader->LoadFile(p_opts.m_queryPath)))//加载查询文件
                {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read query file.\n");
                    return ret;
                }
                auto querySet = queryReader->GetVectorSet();//获取加载后的查询向量集合
                int numQueries = querySet->Count();//查询向量个数

                std::vector<QueryResult> results(numQueries, QueryResult(NULL, max(K, internalResultNum), false));//结果集，每个查询对应一个queryResult对象
                std::vector<SPANN::SearchStats> stats(numQueries);//用于记录每个查询的性能指标
                for (int i = 0; i < numQueries; ++i)
                {
                    (*((COMMON::QueryResultSet<ValueType>*)&results[i])).SetTarget(reinterpret_cast<ValueType*>(querySet->GetVector(i)), p_index->m_pQuantizer);
                    results[i].Reset();//重置结果集
                }


                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Start ANN Search...\n");

                SearchSequential(p_index, numThreads, results, stats, p_opts.m_queryCountLimit, internalResultNum);

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "\nFinish ANN Search...\n");

                std::shared_ptr<VectorSet> vectorSet;//定义一个共享指针，用于存放全量向量集

                if (!p_opts.m_vectorPath.empty() && fileexists(p_opts.m_vectorPath.c_str())) {
                    std::shared_ptr<Helper::ReaderOptions> vectorOptions(new Helper::ReaderOptions(p_opts.m_valueType, p_opts.m_dim, p_opts.m_vectorType, p_opts.m_vectorDelimiter));
                    auto vectorReader = Helper::VectorSetReader::CreateInstance(vectorOptions);
                    if (ErrorCode::Success == vectorReader->LoadFile(p_opts.m_vectorPath))
                    {
                        vectorSet = vectorReader->GetVectorSet();
                        if (p_opts.m_distCalcMethod == DistCalcMethod::Cosine) vectorSet->Normalize(numThreads);
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "\nLoad VectorSet(%d,%d).\n", vectorSet->Count(), vectorSet->Dimension());
                    }
                }

                if (p_opts.m_rerank > 0 && vectorSet != nullptr) {//重排序
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "\n Begin rerank...\n");
                    for (int i = 0; i < results.size(); i++)//遍历每个查询结果
                    {
                        for (int j = 0; j < K; j++)//遍历该查询下的TopK个结果
                        {
                            if (results[i].GetResult(j)->VID < 0) continue;//跳过无效ID
                            results[i].GetResult(j)->Dist = COMMON::DistanceUtils::ComputeDistance((const ValueType*)querySet->GetVector(i),
                                (const ValueType*)vectorSet->GetVector(results[i].GetResult(j)->VID), querySet->Dimension(), p_opts.m_distCalcMethod);//使用原始向量与查询向量重新计算真实距离。
                        }
                        BasicResult* re = results[i].GetResults();
                        std::sort(re, re + K, COMMON::Compare);//将结果重新排序
                    }
                    K = p_opts.m_rerank;
                }

                float recall = 0, MRR = 0;
                std::vector<std::set<SizeType>> truth;
                if (!truthFile.empty())
                {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Start loading TruthFile...\n");

                    auto ptr = f_createIO();
                    if (ptr == nullptr || !ptr->Initialize(truthFile.c_str(), std::ios::in | std::ios::binary)) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed open truth file: %s\n", truthFile.c_str());
                        return ErrorCode::FailedOpenFile;
                    }
                    int originalK = truthK;
                    COMMON::TruthSet::LoadTruth(ptr, truth, numQueries, originalK, truthK, p_opts.m_truthType);//加载真值文件
                    char tmp[4];
                    if (ptr->ReadBinary(4, tmp) == 4) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Truth number is larger than query number(%d)!\n", numQueries);
                    }

                    recall = COMMON::TruthSet::CalculateRecall<ValueType>((p_index->GetMemoryIndex()).get(), results, truth, K, truthK, querySet, vectorSet, numQueries, nullptr, false, &MRR);
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Recall%d@%d: %f MRR@%d: %f\n", truthK, K, recall, K, MRR);
                }

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "\nEx Elements Count:\n");
                PrintPercentiles<double, SPANN::SearchStats>(stats,
                    [](const SPANN::SearchStats& ss) -> double
                    {
                        return ss.m_totalListElementsCount;
                    },
                    "%.3lf");

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "\nHead Latency Distribution:\n");
                PrintPercentiles<double, SPANN::SearchStats>(stats,
                    [](const SPANN::SearchStats& ss) -> double
                    {
                        return ss.m_totalSearchLatency - ss.m_exLatency;
                    },
                    "%.3lf");

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "\nEx Latency Distribution:\n");
                PrintPercentiles<double, SPANN::SearchStats>(stats,
                    [](const SPANN::SearchStats& ss) -> double
                    {
                        return ss.m_exLatency;
                    },
                    "%.3lf");

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "\nTotal Latency Distribution:\n");
                PrintPercentiles<double, SPANN::SearchStats>(stats,
                    [](const SPANN::SearchStats& ss) -> double
                    {
                        return ss.m_totalSearchLatency;
                    },
                    "%.3lf");

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "\nTotal Disk Page Access Distribution:\n");
                PrintPercentiles<int, SPANN::SearchStats>(stats,
                    [](const SPANN::SearchStats& ss) -> int
                    {
                        return ss.m_diskAccessCount;
                    },
                    "%4d");

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "\nTotal Disk IO Distribution:\n");
                PrintPercentiles<int, SPANN::SearchStats>(stats,
                    [](const SPANN::SearchStats& ss) -> int
                    {
                        return ss.m_diskIOCount;
                    },
                    "%4d");

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "\n");

                if (!outputFile.empty())
                {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Start output to %s\n", outputFile.c_str());
                    OutputResult<ValueType>(outputFile, results, K);
                }

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                    "Recall@%d: %f MRR@%d: %f\n", K, recall, K, MRR);

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "\n");

                if (p_opts.m_recall_analysis) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Start recall analysis...\n");

                    std::shared_ptr<VectorIndex> headIndex = p_index->GetMemoryIndex();
                    SizeType sampleSize = numQueries < 100 ? numQueries : 100;
                    SizeType sampleK = headIndex->GetNumSamples() < 1000 ? headIndex->GetNumSamples() : 1000;
                    float sampleE = 1e-6f;

                    std::vector<SizeType> samples(sampleSize, 0);
                    std::vector<float> queryHeadRecalls(sampleSize, 0);
                    std::vector<float> truthRecalls(sampleSize, 0);
                    std::vector<int> shouldSelect(sampleSize, 0);
                    std::vector<int> shouldSelectLong(sampleSize, 0);
                    std::vector<int> nearQueryHeads(sampleSize, 0);
                    std::vector<int> annNotFound(sampleSize, 0);
                    std::vector<int> rngRule(sampleSize, 0);
                    std::vector<int> postingCut(sampleSize, 0);
                    for (int i = 0; i < sampleSize; i++) samples[i] = COMMON::Utils::rand(numQueries);

                    std::vector<std::thread> mythreads;
                    mythreads.reserve(p_opts.m_iSSDNumberOfThreads);
                    std::atomic_size_t sent(0);
                    for (int tid = 0; tid < p_opts.m_iSSDNumberOfThreads; tid++)
                    {
                        mythreads.emplace_back([&, tid]() {
                            size_t i = 0;
                            while (true)
                            {
                                i = sent.fetch_add(1);
                                if (i < sampleSize)
                                {
                                    COMMON::QueryResultSet<ValueType> queryANNHeads(
                                        (const ValueType *)(querySet->GetVector(samples[i])),
                                        max(K, internalResultNum));
                                    headIndex->SearchIndex(queryANNHeads);
                                    float queryANNHeadsLongestDist =
                                        queryANNHeads.GetResult(internalResultNum - 1)->Dist;

                                    COMMON::QueryResultSet<ValueType> queryBFHeads(
                                        (const ValueType *)(querySet->GetVector(samples[i])),
                                        max(sampleK, internalResultNum));
                                    for (SizeType y = 0; y < headIndex->GetNumSamples(); y++)
                                    {
                                        float dist = headIndex->ComputeDistance(queryBFHeads.GetQuantizedTarget(),
                                                                                headIndex->GetSample(y));
                                        queryBFHeads.AddPoint(y, dist);
                                    }
                                    queryBFHeads.SortResult();

                                    {
                                        std::vector<bool> visited(internalResultNum, false);
                                        for (SizeType y = 0; y < internalResultNum; y++)
                                        {
                                            for (SizeType z = 0; z < internalResultNum; z++)
                                            {
                                                if (visited[z])
                                                    continue;

                                                if (fabs(queryANNHeads.GetResult(z)->Dist -
                                                         queryBFHeads.GetResult(y)->Dist) < sampleE)
                                                {
                                                    queryHeadRecalls[i] += 1;
                                                    visited[z] = true;
                                                    break;
                                                }
                                            }
                                        }
                                    }

                                    std::map<int, std::set<int>> tmpFound; // headID->truths
                                    p_index->DebugSearchDiskIndex(queryBFHeads, internalResultNum, sampleK, nullptr,
                                                                  &truth[samples[i]], &tmpFound);

                                    for (SizeType z = 0; z < K; z++)
                                    {
                                        truthRecalls[i] += truth[samples[i]].count(queryBFHeads.GetResult(z)->VID);
                                    }

                                    for (SizeType z = 0; z < K; z++)
                                    {
                                        truth[samples[i]].erase(results[samples[i]].GetResult(z)->VID);
                                    }

                                    for (std::map<int, std::set<int>>::iterator it = tmpFound.begin();
                                         it != tmpFound.end(); it++)
                                    {
                                        float q2truthposting = headIndex->ComputeDistance(
                                            querySet->GetVector(samples[i]), headIndex->GetSample(it->first));
                                        for (auto vid : it->second)
                                        {
                                            if (!truth[samples[i]].count(vid))
                                                continue;

                                            if (q2truthposting < queryANNHeadsLongestDist)
                                                shouldSelect[i] += 1;
                                            else
                                            {
                                                shouldSelectLong[i] += 1;

                                                std::set<int> nearQuerySelectedHeads;
                                                float v2vhead = headIndex->ComputeDistance(
                                                    vectorSet->GetVector(vid), headIndex->GetSample(it->first));
                                                for (SizeType z = 0; z < internalResultNum; z++)
                                                {
                                                    if (queryANNHeads.GetResult(z)->VID < 0)
                                                        break;
                                                    float v2qhead = headIndex->ComputeDistance(
                                                        vectorSet->GetVector(vid),
                                                        headIndex->GetSample(queryANNHeads.GetResult(z)->VID));
                                                    if (v2qhead < v2vhead)
                                                    {
                                                        nearQuerySelectedHeads.insert(queryANNHeads.GetResult(z)->VID);
                                                    }
                                                }
                                                if (nearQuerySelectedHeads.size() == 0)
                                                    continue;

                                                nearQueryHeads[i] += 1;

                                                COMMON::QueryResultSet<ValueType> annTruthHead(
                                                    (const ValueType *)(vectorSet->GetVector(vid)),
                                                    p_opts.m_debugBuildInternalResultNum);
                                                headIndex->SearchIndex(annTruthHead);

                                                bool found = false;
                                                for (SizeType z = 0; z < annTruthHead.GetResultNum(); z++)
                                                {
                                                    if (nearQuerySelectedHeads.count(annTruthHead.GetResult(z)->VID))
                                                    {
                                                        found = true;
                                                        break;
                                                    }
                                                }

                                                if (!found)
                                                {
                                                    annNotFound[i] += 1;
                                                    continue;
                                                }

                                                // RNG rule and posting cut
                                                std::set<int> replicas;
                                                for (SizeType z = 0; z < annTruthHead.GetResultNum() &&
                                                                     replicas.size() < p_opts.m_replicaCount;
                                                     z++)
                                                {
                                                    BasicResult *item = annTruthHead.GetResult(z);
                                                    if (item->VID < 0)
                                                        break;

                                                    bool good = true;
                                                    for (auto r : replicas)
                                                    {
                                                        if (p_opts.m_rngFactor * headIndex->ComputeDistance(
                                                                                     headIndex->GetSample(r),
                                                                                     headIndex->GetSample(item->VID)) <
                                                            item->Dist)
                                                        {
                                                            good = false;
                                                            break;
                                                        }
                                                    }
                                                    if (good)
                                                        replicas.insert(item->VID);
                                                }

                                                found = false;
                                                for (auto r : nearQuerySelectedHeads)
                                                {
                                                    if (replicas.count(r))
                                                    {
                                                        found = true;
                                                        break;
                                                    }
                                                }

                                                if (found)
                                                    postingCut[i] += 1;
                                                else
                                                    rngRule[i] += 1;
                                            }
                                        }
                                    }
                                }
                                else
                                {
                                    return;
                                }
                            }
                        });
                    }
                    for (auto &t : mythreads)
                    {
                        t.join();
                    }
                    mythreads.clear();

                    float headacc = 0, truthacc = 0, shorter = 0, longer = 0, lost = 0, buildNearQueryHeads = 0, buildAnnNotFound = 0, buildRNGRule = 0, buildPostingCut = 0;
                    for (int i = 0; i < sampleSize; i++) {
                        headacc += queryHeadRecalls[i];
                        truthacc += truthRecalls[i];

                        lost += shouldSelect[i] + shouldSelectLong[i];
                        shorter += shouldSelect[i];
                        longer += shouldSelectLong[i];

                        buildNearQueryHeads += nearQueryHeads[i];
                        buildAnnNotFound += annNotFound[i];
                        buildRNGRule += rngRule[i];
                        buildPostingCut += postingCut[i];
                    }

                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Query head recall @%d:%f.\n", internalResultNum, headacc / sampleSize / internalResultNum);
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "BF top %d postings truth recall @%d:%f.\n", sampleK, truthK, truthacc / sampleSize / truthK);

                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                        "Percent of truths in postings have shorter distance than query selected heads: %f percent\n",
                        shorter / lost * 100);
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                        "Percent of truths in postings have longer distance than query selected heads: %f percent\n",
                        longer / lost * 100);


                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                        "\tPercent of truths no shorter distance in query selected heads: %f percent\n",
                        (longer - buildNearQueryHeads) / lost * 100);
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                        "\tPercent of truths exists shorter distance in query selected heads: %f percent\n",
                        buildNearQueryHeads / lost * 100);

                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                        "\t\tRNG rule ANN search loss: %f percent\n", buildAnnNotFound / lost * 100);
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                        "\t\tPosting cut loss: %f percent\n", buildPostingCut / lost * 100);
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                        "\t\tRNG rule loss: %f percent\n", buildRNGRule / lost * 100);
                }
                return ErrorCode::Success;
            }
		}
	}
}
