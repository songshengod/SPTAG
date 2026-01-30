// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SPANN_EXTRASTATICSEARCHER_H_
#define _SPTAG_SPANN_EXTRASTATICSEARCHER_H_

#include "inc/Helper/VectorSetReader.h"
#include "inc/Helper/AsyncFileReader.h"
#include "IExtraSearcher.h"
#include "inc/Core/Common/TruthSet.h"
#include "Compressor.h"

#include <map>
#include <cmath>
#include <climits>
#include <future>
#include <numeric>

namespace SPTAG
{
    namespace SPANN
    {
        extern std::function<std::shared_ptr<Helper::DiskIO>(void)> f_createAsyncIO;

        struct Selection {
            std::string m_tmpfile;
            size_t m_totalsize;
            size_t m_start;
            size_t m_end;
            std::vector<Edge> m_selections;
            static EdgeCompare g_edgeComparer;

            Selection(size_t totalsize, std::string tmpdir) : m_tmpfile(tmpdir + FolderSep + "selection_tmp"), m_totalsize(totalsize), m_start(0), m_end(totalsize) { remove(m_tmpfile.c_str()); m_selections.resize(totalsize); }

            ErrorCode SaveBatch()
            {
                auto f_out = f_createIO();
                if (f_out == nullptr || !f_out->Initialize(m_tmpfile.c_str(), std::ios::out | std::ios::binary | (fileexists(m_tmpfile.c_str()) ? std::ios::in : 0))) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Cannot open %s to save selection for batching!\n", m_tmpfile.c_str());
                    return ErrorCode::FailedOpenFile;
                }
                if (f_out->WriteBinary(sizeof(Edge) * (m_end - m_start), (const char*)m_selections.data(), sizeof(Edge) * m_start) != sizeof(Edge) * (m_end - m_start)) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Cannot write to %s!\n", m_tmpfile.c_str());
                    return ErrorCode::DiskIOFail;
                }
                std::vector<Edge> batch_selection;
                m_selections.swap(batch_selection);
                m_start = m_end = 0;
                return ErrorCode::Success;
            }

            ErrorCode LoadBatch(size_t start, size_t end)
            {
                auto f_in = f_createIO();
                if (f_in == nullptr || !f_in->Initialize(m_tmpfile.c_str(), std::ios::in | std::ios::binary)) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Cannot open %s to load selection batch!\n", m_tmpfile.c_str());
                    return ErrorCode::FailedOpenFile;
                }

                size_t readsize = end - start;
                m_selections.resize(readsize);
                if (f_in->ReadBinary(readsize * sizeof(Edge), (char*)m_selections.data(), start * sizeof(Edge)) != readsize * sizeof(Edge)) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Cannot read from %s! start:%zu size:%zu\n", m_tmpfile.c_str(), start, readsize);
                    return ErrorCode::DiskIOFail;
                }
                m_start = start;
                m_end = end;
                return ErrorCode::Success;
            }

            size_t lower_bound(SizeType node)
            {
                auto ptr = std::lower_bound(m_selections.begin(), m_selections.end(), node, g_edgeComparer);
                return m_start + (ptr - m_selections.begin());
            }

            Edge& operator[](size_t offset)
            {
                if (offset < m_start || offset >= m_end) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Error read offset in selections:%zu\n", offset);
                }
                return m_selections[offset - m_start];
            }
        };

#define DecompressPosting(){\
        p_postingListFullData = (char*)p_exWorkSpace->m_decompressBuffer.GetBuffer(); \
        if (listInfo->listEleCount != 0) { \
            std::size_t sizePostingListFullData;\
            try {\
                sizePostingListFullData = m_pCompressor->Decompress(buffer + listInfo->pageOffset, listInfo->listTotalBytes, p_postingListFullData, listInfo->listEleCount * m_vectorInfoSize, m_enableDictTraining);\
            }\
            catch (std::runtime_error& err) {\
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Decompress postingList %d  failed! %s, \n", listInfo - m_listInfos.data(), err.what());\
                return;\
            }\
            if (sizePostingListFullData != listInfo->listEleCount * m_vectorInfoSize) {\
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "PostingList %d decompressed size not match! %zu, %d, \n", listInfo - m_listInfos.data(), sizePostingListFullData, listInfo->listEleCount * m_vectorInfoSize);\
                return;\
            }\
        }\
}\

#define DecompressPostingIterative(){\
        p_postingListFullData = (char*)p_exWorkSpace->m_decompressBuffer.GetBuffer(); \
        if (listInfo->listEleCount != 0) { \
            std::size_t sizePostingListFullData;\
            try {\
                sizePostingListFullData = m_pCompressor->Decompress(buffer + listInfo->pageOffset, listInfo->listTotalBytes, p_postingListFullData, listInfo->listEleCount * m_vectorInfoSize, m_enableDictTraining);\
                if (sizePostingListFullData != listInfo->listEleCount * m_vectorInfoSize) {\
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "PostingList %d decompressed size not match! %zu, %d, \n", listInfo - m_listInfos.data(), sizePostingListFullData, listInfo->listEleCount * m_vectorInfoSize);\
                }\
             }\
            catch (std::runtime_error& err) {\
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Decompress postingList %d  failed! %s, \n", listInfo - m_listInfos.data(), err.what());\
            }\
        }\
}\

#define ProcessPosting() \
        for (int i = 0; i < listInfo->listEleCount; i++) { \
            uint64_t offsetVectorID, offsetVector;\
            (this->*m_parsePosting)(offsetVectorID, offsetVector, i, listInfo->listEleCount);\
            int vectorID = *(reinterpret_cast<int*>(p_postingListFullData + offsetVectorID));\
            if (p_exWorkSpace->m_deduper.CheckAndSet(vectorID)) { listElements--; continue; } \
            (this->*m_parseEncoding)(p_index, listInfo, (ValueType*)(p_postingListFullData + offsetVector));\
            auto distance2leaf = p_index->ComputeDistance(queryResults.GetQuantizedTarget(), p_postingListFullData + offsetVector); \
            queryResults.AddPoint(vectorID, distance2leaf); \
        } \

#define ProcessPostingOffset() \
        while (p_exWorkSpace->m_offset < listInfo->listEleCount) { \
            uint64_t offsetVectorID, offsetVector;\
            (this->*m_parsePosting)(offsetVectorID, offsetVector, p_exWorkSpace->m_offset, listInfo->listEleCount);\
            p_exWorkSpace->m_offset++;\
            int vectorID = *(reinterpret_cast<int*>(p_postingListFullData + offsetVectorID));\
            if (p_exWorkSpace->m_deduper.CheckAndSet(vectorID)) continue; \
            if (p_exWorkSpace->m_filterFunc != nullptr && !p_exWorkSpace->m_filterFunc(p_spann->GetMetadata(vectorID))) continue; \
            (this->*m_parseEncoding)(p_index, listInfo, (ValueType*)(p_postingListFullData + offsetVector));\
            auto distance2leaf = p_index->ComputeDistance(queryResults.GetQuantizedTarget(), p_postingListFullData + offsetVector); \
            queryResults.AddPoint(vectorID, distance2leaf); \
            foundResult = true;\
            break;\
        } \
        if (p_exWorkSpace->m_offset == listInfo->listEleCount) { \
            p_exWorkSpace->m_pi++; \
            p_exWorkSpace->m_offset = 0; \
        } \

        template <typename ValueType>
        class ExtraStaticSearcher : public IExtraSearcher
        {
        public:
            ExtraStaticSearcher()
            {
                m_enableDeltaEncoding = false;
                m_enablePostingListRearrange = false;
                m_enableDataCompression = false;
                m_enableDictTraining = true;
            }

            virtual ~ExtraStaticSearcher()
            {
            }

            virtual bool Available() override
            {
                return m_available;
            }

            void InitWorkSpace(ExtraWorkSpace* p_exWorkSpace, bool clear = false) override
            {
                if (clear) {
                    p_exWorkSpace->Clear(m_opt->m_searchInternalResultNum, max(m_opt->m_postingPageLimit, m_opt->m_searchPostingPageLimit + 1) << PageSizeEx, false, m_opt->m_enableDataCompression);
                }
                else {
                    p_exWorkSpace->Initialize(m_opt->m_maxCheck, m_opt->m_hashExp, m_opt->m_searchInternalResultNum, max(m_opt->m_postingPageLimit, m_opt->m_searchPostingPageLimit + 1) << PageSizeEx, false, m_opt->m_enableDataCompression);
                    int wid = 0;
                    if (m_freeWorkSpaceIds == nullptr || !m_freeWorkSpaceIds->try_pop(wid))
                    {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "FreeWorkSpaceIds is not initalized or the workspace number is not enough! Please increase iothread number.\n");
                        wid = m_workspaceCount.fetch_add(1);
                    }
                    for (auto & req : p_exWorkSpace->m_diskRequests)
                    {
                        req.m_status = wid;
                    }
                    p_exWorkSpace->m_callback = [m_freeWorkSpaceIds = m_freeWorkSpaceIds, wid] () {
                        if (m_freeWorkSpaceIds) m_freeWorkSpaceIds->push(wid);
                    };
                }
            }

            virtual bool LoadIndex(Options& p_opt, COMMON::VersionLabel& p_versionMap, COMMON::Dataset<std::uint64_t>& p_vectorTranslateMap,  std::shared_ptr<VectorIndex> m_index) {
                m_extraFullGraphFile = p_opt.m_indexDirectory + FolderSep + p_opt.m_ssdIndex;
                std::string curFile = m_extraFullGraphFile;
                p_opt.m_searchPostingPageLimit = max(p_opt.m_searchPostingPageLimit, static_cast<int>((p_opt.m_postingVectorLimit * (p_opt.m_dim * sizeof(ValueType) + sizeof(int)) + PageSize - 1) / PageSize));//计算：加载一个pl最坏需要多少磁盘页
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Load index with posting page limit:%d\n", p_opt.m_searchPostingPageLimit);//搜索时，读一个pl，最坏要读多少个Page
                do {
                        auto curIndexFile = f_createAsyncIO();
                    if (curIndexFile == nullptr || !curIndexFile->Initialize(curFile.c_str(),
#ifndef _MSC_VER
#ifdef BATCH_READ
                        O_RDONLY | O_DIRECT, p_opt.m_searchInternalResultNum, 2, 2, p_opt.m_iSSDNumberOfThreads
#else
                        O_RDONLY | O_DIRECT, p_opt.m_searchInternalResultNum * p_opt.m_iSSDNumberOfThreads / p_opt.m_ioThreads + 1, 2, 2, p_opt.m_ioThreads
#endif
#else
                        GENERIC_READ, (p_opt.m_searchPostingPageLimit + 1) * PageSize, 2, 2, (std::uint16_t)p_opt.m_ioThreads
#endif
                    )) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Cannot open file:%s!\n", curFile.c_str());
                        return false;
                    }

                    m_indexFiles.emplace_back(curIndexFile);
                    try {
                        m_totalListCount += LoadingHeadInfo(curFile, p_opt.m_searchPostingPageLimit, m_listInfos);//加载每个pl的头部的元数据，解析出每个pl在磁盘上的起始偏移量和长度。
                    } 
                    catch (std::exception& e)
                    {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Error occurs when loading HeadInfo:%s\n", e.what());
                        return false;
                    }

                    curFile = m_extraFullGraphFile + "_" + std::to_string(m_indexFiles.size());
                } while (fileexists(curFile.c_str()));//SPANN支持将巨大的磁盘索引拆分成多个分片文件。
                m_oneContext = (m_indexFiles.size() == 1);

                m_opt = &p_opt;
                m_enableDeltaEncoding = p_opt.m_enableDeltaEncoding;
                m_enablePostingListRearrange = p_opt.m_enablePostingListRearrange;
                m_enableDataCompression = p_opt.m_enableDataCompression;
                m_enableDictTraining = p_opt.m_enableDictTraining;

                if (m_enablePostingListRearrange) m_parsePosting = &ExtraStaticSearcher<ValueType>::ParsePostingListRearrange;
                else m_parsePosting = &ExtraStaticSearcher<ValueType>::ParsePostingList;
                if (m_enableDeltaEncoding) m_parseEncoding = &ExtraStaticSearcher<ValueType>::ParseDeltaEncoding;
                else m_parseEncoding = &ExtraStaticSearcher<ValueType>::ParseEncoding;
                
                m_listPerFile = static_cast<int>((m_totalListCount + m_indexFiles.size() - 1) / m_indexFiles.size());

                p_versionMap.Load(p_opt.m_indexDirectory + FolderSep + p_opt.m_deleteIDFile, p_opt.m_datasetRowsInBlock, p_opt.m_datasetCapacity);//加载逻辑删除ID，SPANN需要知道哪些向量已经被删除
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Current vector num: %d.\n", p_versionMap.Count());

#ifndef _MSC_VER
                Helper::AIOTimeout.tv_nsec = p_opt.m_iotimeout * 1000;
#endif

                m_freeWorkSpaceIds.reset(new Helper::Concurrent::ConcurrentQueue<int>());
                int maxIOThreads = max(p_opt.m_searchThreadNum, p_opt.m_iSSDNumberOfThreads);
                for (int i = 0; i < maxIOThreads; i++) {
                    m_freeWorkSpaceIds->push(i);
                }
                m_workspaceCount = maxIOThreads;
                m_available = true;
                return true;
            }

            virtual ErrorCode SearchIndex(ExtraWorkSpace* p_exWorkSpace,
                QueryResult& p_queryResults,
                std::shared_ptr<VectorIndex> p_index,
                SearchStats* p_stats,
                std::set<int>* truth, std::map<int, std::set<int>>* found)//从SSD异步读取postinglist数据并计算最终的向量距离
            {
                const uint32_t postingListCount = static_cast<uint32_t>(p_exWorkSpace->m_postingIDs.size());

                COMMON::QueryResultSet<ValueType>& queryResults = *((COMMON::QueryResultSet<ValueType>*)&p_queryResults);
 
                int diskRead = 0;
                int diskIO = 0;
                int listElements = 0;

#if defined(ASYNC_READ) && !defined(BATCH_READ)
                int unprocessed = 0;
#endif

                for (uint32_t pi = 0; pi < postingListCount; ++pi)
                {
                    auto curPostingID = p_exWorkSpace->m_postingIDs[pi];
                    ListInfo* listInfo = &(m_listInfos[curPostingID]);
                    int fileid = m_oneContext? 0: curPostingID / m_listPerFile;

#ifndef BATCH_READ
                    Helper::DiskIO* indexFile = m_indexFiles[fileid].get();
#endif

                    diskRead += listInfo->listPageCount;
                    diskIO += 1;
                    listElements += listInfo->listEleCount;

                    size_t totalBytes = (static_cast<size_t>(listInfo->listPageCount) << PageSizeEx);

#ifdef ASYNC_READ       
                    auto& request = p_exWorkSpace->m_diskRequests[pi];
                    request.m_offset = listInfo->listOffset;
                    request.m_readSize = totalBytes;
                    request.m_status = (fileid << 16) | (request.m_status & 0xffff);
                    request.m_payload = (void*)listInfo; 
                    request.m_success = false;

#ifdef BATCH_READ // async batch read
                    request.m_callback = [&p_exWorkSpace, &queryResults, &p_index, &request, &listElements, this](bool success)
                    {
                        char* buffer = request.m_buffer;
                        ListInfo* listInfo = (ListInfo*)(request.m_payload);

                        // decompress posting list
                        char* p_postingListFullData = buffer + listInfo->pageOffset;
                        if (m_enableDataCompression)
                        {
                            DecompressPosting();
                        }

                        ProcessPosting();
                    };
#else // async read
                    request.m_callback = [&p_exWorkSpace, &request](bool success)
                    {
                        p_exWorkSpace->m_processIocp.push(&request);
                    };

                    ++unprocessed;
                    if (!(indexFile->ReadFileAsync(request)))
                    {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read file!\n");
                        unprocessed--;
                    }
#endif
#else // sync read
                    char* buffer = (char*)((p_exWorkSpace->m_pageBuffers[pi]).GetBuffer());
                    auto numRead = indexFile->ReadBinary(totalBytes, buffer, listInfo->listOffset);
                    if (numRead != totalBytes) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "File %s read bytes, expected: %zu, acutal: %llu.\n", m_extraFullGraphFile.c_str(), totalBytes, numRead);
                        throw std::runtime_error("File read mismatch");
                    }
                    // decompress posting list
                    char* p_postingListFullData = buffer + listInfo->pageOffset;
                    if (m_enableDataCompression)
                    {
                        DecompressPosting();
                    }

                    ProcessPosting();
#endif
                }

#ifdef ASYNC_READ
#ifdef BATCH_READ
                BatchReadFileAsync(m_indexFiles, (p_exWorkSpace->m_diskRequests).data(), postingListCount);
#else
                while (unprocessed > 0)
                {
                    Helper::AsyncReadRequest* request;
                    if (!(p_exWorkSpace->m_processIocp.pop(request))) break;

                    --unprocessed;
                    char* buffer = request->m_buffer;
                    ListInfo* listInfo = static_cast<ListInfo*>(request->m_payload);
                    // decompress posting list
                    char* p_postingListFullData = buffer + listInfo->pageOffset;
                    if (m_enableDataCompression)
                    {
                        DecompressPosting();
                    }

                    ProcessPosting();
                }
#endif
#endif
                if (truth) {
                    for (uint32_t pi = 0; pi < postingListCount; ++pi)
                    {
                        auto curPostingID = p_exWorkSpace->m_postingIDs[pi];

                        ListInfo* listInfo = &(m_listInfos[curPostingID]);
                        char* buffer = (char*)((p_exWorkSpace->m_pageBuffers[pi]).GetBuffer());

                        char* p_postingListFullData = buffer + listInfo->pageOffset;
                        if (m_enableDataCompression)
                        {
                            p_postingListFullData = (char*)p_exWorkSpace->m_decompressBuffer.GetBuffer();
                            if (listInfo->listEleCount != 0)
                            {
                                try {
                                    m_pCompressor->Decompress(buffer + listInfo->pageOffset, listInfo->listTotalBytes, p_postingListFullData, listInfo->listEleCount * m_vectorInfoSize, m_enableDictTraining);
                                }
                                catch (std::runtime_error& err) {
                                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Decompress postingList %d  failed! %s, \n", curPostingID, err.what());
                                    continue;
                                }
                            }
                        }

                        for (size_t i = 0; i < listInfo->listEleCount; ++i) {
                            uint64_t offsetVectorID = m_enablePostingListRearrange ? (m_vectorInfoSize - sizeof(int)) * listInfo->listEleCount + sizeof(int) * i : m_vectorInfoSize * i; \
                            int vectorID = *(reinterpret_cast<int*>(p_postingListFullData + offsetVectorID)); \
                            if (truth && truth->count(vectorID)) (*found)[curPostingID].insert(vectorID);
                        }
                    }
                }

                if (p_stats) 
                {
                    p_stats->m_totalListElementsCount = listElements;
                    p_stats->m_diskIOCount = diskIO;
                    p_stats->m_diskAccessCount = diskRead;
                }
                queryResults.SetScanned(listElements);
                return ErrorCode::Success;
            }

            virtual ErrorCode SearchIndexWithoutParsing(ExtraWorkSpace *p_exWorkSpace) override
            {
                const uint32_t postingListCount = static_cast<uint32_t>(p_exWorkSpace->m_postingIDs.size());

                int diskRead = 0;
                int diskIO = 0;
                int listElements = 0;

#if defined(ASYNC_READ) && !defined(BATCH_READ)
                int unprocessed = 0;
#endif

                for (uint32_t pi = 0; pi < postingListCount; ++pi)
                {
                    auto curPostingID = p_exWorkSpace->m_postingIDs[pi];
                    ListInfo* listInfo = &(m_listInfos[curPostingID]);
                    int fileid = m_oneContext ? 0 : curPostingID / m_listPerFile;

#ifndef BATCH_READ
                    Helper::DiskIO* indexFile = m_indexFiles[fileid].get();
#endif

                    diskRead += listInfo->listPageCount;
                    diskIO += 1;
                    listElements += listInfo->listEleCount;

                    size_t totalBytes = (static_cast<size_t>(listInfo->listPageCount) << PageSizeEx);
                    
#ifdef ASYNC_READ       
                    auto& request = p_exWorkSpace->m_diskRequests[pi];
                    request.m_offset = listInfo->listOffset;
                    request.m_readSize = totalBytes;
                    request.m_status = (fileid << 16) | (request.m_status & 0xffff);
                    request.m_payload = (void*)listInfo;
                    request.m_success = false;

#ifdef BATCH_READ // async batch read
                    request.m_callback = [this](bool success)
                    {
                        //char* buffer = request.m_buffer;
                        //ListInfo* listInfo = (ListInfo*)(request.m_payload);

                        // decompress posting list
                        /*
                        char* p_postingListFullData = buffer + listInfo->pageOffset;
                        if (m_enableDataCompression)
                        {
                            DecompressPosting();
                        }

                        ProcessPosting();
                        */
                    };
#else // async read
                    request.m_callback = [&p_exWorkSpace, &request](bool success)
                    {
                        p_exWorkSpace->m_processIocp.push(&request);
                    };

                    ++unprocessed;
                    if (!(indexFile->ReadFileAsync(request)))
                    {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read file!\n");
                        unprocessed--;
                    }
#endif
#else // sync read
                    char* buffer = (char*)((p_exWorkSpace->m_pageBuffers[pi]).GetBuffer());
                    auto numRead = indexFile->ReadBinary(totalBytes, buffer, listInfo->listOffset);
                    if (numRead != totalBytes) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "File %s read bytes, expected: %zu, acutal: %llu.\n", m_extraFullGraphFile.c_str(), totalBytes, numRead);
                        return ErrorCode::DiskIOFail;
                    }
                    // decompress posting list
                    /*
                    char* p_postingListFullData = buffer + listInfo->pageOffset;
                    if (m_enableDataCompression)
                    {
                        DecompressPosting();
                    }

                    ProcessPosting();
                    */
#endif
                }

#ifdef ASYNC_READ
#ifdef BATCH_READ
                int retry = 0;
                bool success = false;
                while (retry < 2 && !success)
                {
                    success = BatchReadFileAsync(m_indexFiles, (p_exWorkSpace->m_diskRequests).data(), postingListCount);
                    retry++;
                }
#else
                while (unprocessed > 0)
                {
                    Helper::AsyncReadRequest* request;
                    if (!(p_exWorkSpace->m_processIocp.pop(request))) break;

                    --unprocessed;
                    char* buffer = request->m_buffer;
                    ListInfo* listInfo = static_cast<ListInfo*>(request->m_payload);
                    // decompress posting list
                    /*
                    char* p_postingListFullData = buffer + listInfo->pageOffset;
                    if (m_enableDataCompression)
                    {
                        DecompressPosting();
                    }

                    ProcessPosting();
                    */
                }
#endif
#endif
                return (success)? ErrorCode::Success: ErrorCode::DiskIOFail;
            }

            virtual ErrorCode SearchNextInPosting(ExtraWorkSpace* p_exWorkSpace, QueryResult& p_headResults,
                QueryResult& p_queryResults,
		        std::shared_ptr<VectorIndex>& p_index, const VectorIndex* p_spann) override
            {
                COMMON::QueryResultSet<ValueType>& headResults = *((COMMON::QueryResultSet<ValueType>*) & p_headResults);
                COMMON::QueryResultSet<ValueType>& queryResults = *((COMMON::QueryResultSet<ValueType>*) & p_queryResults);
                bool foundResult = false;
                BasicResult* head = headResults.GetResult(p_exWorkSpace->m_ri);
                while (!foundResult && p_exWorkSpace->m_pi < p_exWorkSpace->m_postingIDs.size()) {
                    if (head && head->VID != -1 && p_exWorkSpace->m_ri <= p_exWorkSpace->m_pi &&
                       (p_exWorkSpace->m_filterFunc == nullptr || p_exWorkSpace->m_filterFunc(p_spann->GetMetadata(head->VID)))) {
                        queryResults.AddPoint(head->VID, head->Dist);
                        head = headResults.GetResult(++p_exWorkSpace->m_ri);
                        foundResult = true;
                        continue;
                    }
                    char* buffer = (char*)((p_exWorkSpace->m_pageBuffers[p_exWorkSpace->m_pi]).GetBuffer());
                    ListInfo* listInfo = static_cast<ListInfo*>(p_exWorkSpace->m_diskRequests[p_exWorkSpace->m_pi].m_payload);
                    // decompress posting list
                    char* p_postingListFullData = buffer + listInfo->pageOffset;
                    if (m_enableDataCompression && p_exWorkSpace->m_offset == 0)
                    {
                        DecompressPostingIterative();
                    }
                    ProcessPostingOffset();
                }
                if (!foundResult && head && head->VID != -1 &&
                (p_exWorkSpace->m_filterFunc == nullptr || p_exWorkSpace->m_filterFunc(p_spann->GetMetadata(head->VID)))) {
                    queryResults.AddPoint(head->VID, head->Dist);
                    head = headResults.GetResult(++p_exWorkSpace->m_ri);
                    foundResult = true;
                }
                if (foundResult) p_queryResults.SetScanned(p_queryResults.GetScanned() + 1);
                return (foundResult)? ErrorCode::Success : ErrorCode::VectorNotFound;
            }

            virtual ErrorCode SearchIterativeNext(ExtraWorkSpace* p_exWorkSpace, QueryResult& p_headResults,
                QueryResult& p_query,
		        std::shared_ptr<VectorIndex> p_index, const VectorIndex* p_spann) override
            {
                if (p_exWorkSpace->m_loadPosting) {
                    ErrorCode ret = SearchIndexWithoutParsing(p_exWorkSpace);
                    if (ret != ErrorCode::Success) return ret;
                    p_exWorkSpace->m_ri = 0;
                    p_exWorkSpace->m_pi = 0;
                    p_exWorkSpace->m_offset = 0;
                    p_exWorkSpace->m_loadPosting = false;
                }

                return SearchNextInPosting(p_exWorkSpace, p_headResults, p_query, p_index, p_spann);
            }

            std::string GetPostingListFullData(
                int postingListId,
                size_t p_postingListSize,
                Selection &p_selections,
                std::shared_ptr<VectorSet> p_fullVectors,
                bool p_enableDeltaEncoding = false,
                bool p_enablePostingListRearrange = false,
                const ValueType *headVector = nullptr)
            {
                std::string postingListFullData("");
                std::string vectors("");
                std::string vectorIDs("");
                size_t selectIdx = p_selections.lower_bound(postingListId);//该postinglist在Selection数组中的起始位置
                // iterate over all the vectors in the posting list
                for (int i = 0; i < p_postingListSize; ++i)
                {
                    if (p_selections[selectIdx].node != postingListId)//检查Selection是否匹配
                    {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Selection ID NOT MATCH! node:%d offset:%zu\n", postingListId, selectIdx);
                        throw std::runtime_error("Selection ID mismatch");
                    }
                    std::string vectorID("");
                    std::string vector("");

                    int vid = p_selections[selectIdx++].tonode;
                    vectorID.append(reinterpret_cast<char *>(&vid), sizeof(int));

                    ValueType *p_vector = reinterpret_cast<ValueType *>(p_fullVectors->GetVector(vid));
                    if (p_enableDeltaEncoding)//跳过
                    {
                        DimensionType n = p_fullVectors->Dimension();
                        std::vector<ValueType> p_vector_delta(n);
                        for (auto j = 0; j < n; j++)
                        {
                            p_vector_delta[j] = p_vector[j] - headVector[j];
                        }
                        vector.append(reinterpret_cast<char *>(&p_vector_delta[0]), p_fullVectors->PerVectorDataSize());
                    }
                    else
                    {
                        vector.append(reinterpret_cast<char *>(p_vector), p_fullVectors->PerVectorDataSize());
                    }

                    if (p_enablePostingListRearrange)//跳过
                    {
                        vectorIDs += vectorID;
                        vectors += vector;
                    }
                    else
                    {
                        postingListFullData += (vectorID + vector);
                    }
                }
                if (p_enablePostingListRearrange)
                {
                    return vectors + vectorIDs;
                }
                return postingListFullData;
            }

            bool BuildIndex(std::shared_ptr<Helper::VectorSetReader>& p_reader, std::shared_ptr<VectorIndex> p_headIndex, Options& p_opt, COMMON::VersionLabel& p_versionMap, COMMON::Dataset<std::uint64_t>& p_vectorTranslateMap, SizeType upperBound = -1) {
                std::string outputFile = p_opt.m_indexDirectory + FolderSep + p_opt.m_ssdIndex;//设置输出路径
                if (outputFile.empty())
                {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Output file can't be empty!\n");
                    return false;
                }

                m_opt = &p_opt;
                int numThreads = p_opt.m_iSSDNumberOfThreads;
                int candidateNum = p_opt.m_internalResultNum;
                std::unordered_map<SizeType, SizeType> headVectorIDS;
                if (p_opt.m_headIDFile.empty()) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Not found VectorIDTranslate!\n");
                    return false;
                }

                for (int i = 0; i < p_vectorTranslateMap.R(); i++)
                {
                    headVectorIDS[static_cast<SizeType>(*(p_vectorTranslateMap[i]))] = i;//headvectorids存储了哪些向量是头结点。
                }
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Loaded %u Vector IDs\n", static_cast<uint32_t>(headVectorIDS.size()));

                SizeType fullCount = 0;
                size_t vectorInfoSize = 0;
                {
                    auto fullVectors = p_reader->GetVectorSet();
                    fullCount = fullVectors->Count();//获取向量总数
                    vectorInfoSize = fullVectors->PerVectorDataSize() + sizeof(int);//一个向量的大小
                }
                if (upperBound > 0) fullCount = upperBound;

                p_versionMap.Initialize(fullCount, p_headIndex->m_iDataBlockSize, p_headIndex->m_iDataCapacity);

                Selection selections(static_cast<size_t>(fullCount) * p_opt.m_replicaCount, p_opt.m_tmpdir);//初始化Selection数组：1B*8个副本==80亿规模的数组
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Full vector count:%d Edge bytes:%llu selection size:%zu, capacity size:%zu\n", fullCount, sizeof(Edge), selections.m_selections.size(), selections.m_selections.capacity());
                std::vector<std::atomic_int> replicaCount(fullCount);//记录每个点被分配了多少次
                std::vector<std::atomic_int> postingListSize(p_headIndex->GetNumSamples());//记录每个中心点下有多少个向量
                for (auto& pls : postingListSize) pls = 0;
                std::unordered_set<SizeType> emptySet;
                SizeType batchSize = (fullCount + p_opt.m_batches - 1) / p_opt.m_batches;//计算每一批处理多少条向量

                
                auto t1 = std::chrono::high_resolution_clock::now();
                if (p_opt.m_batches > 1)
                {
                    if (selections.SaveBatch() != ErrorCode::Success)
                    {
                        return false;
                    }
                }
                {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Preparation done, start candidate searching.\n");
                    SizeType sampleSize = p_opt.m_samples;
                    std::vector<SizeType> samples(sampleSize, 0);
                    for (int i = 0; i < p_opt.m_batches; i++) {
                        SizeType start = i * batchSize;
                        SizeType end = min(start + batchSize, fullCount);
                        auto fullVectors = p_reader->GetVectorSet(start, end);//获取当前批次
                        if (p_opt.m_distCalcMethod == DistCalcMethod::Cosine && !p_reader->IsNormalized() && !p_headIndex->m_pQuantizer) fullVectors->Normalize(p_opt.m_iSSDNumberOfThreads);

                        if (p_opt.m_batches > 1) {
                            if (selections.LoadBatch(static_cast<size_t>(start) * p_opt.m_replicaCount, static_cast<size_t>(end) * p_opt.m_replicaCount) != ErrorCode::Success)
                            {
                                return false;
                            }
                            emptySet.clear();
                            for (auto& pair : headVectorIDS) {
                                if (pair.first >= start && pair.first < end) emptySet.insert(pair.first - start);
                            }
                        }
                        else {
                            for (auto& pair : headVectorIDS) {
                                emptySet.insert(pair.first);
                            }
                        }

                        int sampleNum = 0;
                        for (int j = start; j < end && sampleNum < sampleSize; j++)
                        {
                            if (headVectorIDS.count(j) == 0) samples[sampleNum++] = j - start;
                        }

                        float acc = 0;
                        for (int j = 0; j < sampleNum; j++)//随机抽取样本，看看内存里的HeadIndex搜索的向量准不准。
                        {
                            COMMON::Utils::atomic_float_add(&acc, COMMON::TruthSet::CalculateRecall(p_headIndex.get(), fullVectors->GetVector(samples[j]), candidateNum));
                        }
                        acc = acc / sampleNum;
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Batch %d vector(%d,%d) loaded with %d vectors (%zu) HeadIndex acc @%d:%f.\n", i, start, end, fullVectors->Count(), selections.m_selections.size(), candidateNum, acc);

                        p_headIndex->ApproximateRNG(fullVectors, emptySet, candidateNum, selections.m_selections.data(), p_opt.m_replicaCount, numThreads, p_opt.m_gpuSSDNumTrees, p_opt.m_gpuSSDLeafSize, p_opt.m_rngFactor, p_opt.m_numGPUs);
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Batch %d finished!\n", i);

                        for (SizeType j = start; j < end; j++) {
                            replicaCount[j] = 0;
                            size_t vecOffset = j * (size_t)p_opt.m_replicaCount;
                            if (headVectorIDS.count(j) == 0) {//向量是普通向量
                                for (int resNum = 0; resNum < p_opt.m_replicaCount && selections[vecOffset + resNum].node != INT_MAX; resNum++) {
                                    ++postingListSize[selections[vecOffset + resNum].node];//postinglistsize是一个计数器数组，该HeadID的数量加一
                                    selections[vecOffset + resNum].tonode = j;//向量ID对应原始ID，记录目标向量的索引ID
                                    ++replicaCount[j];
                                }
                            } else if (!p_opt.m_excludehead) {
                                selections[vecOffset].node = headVectorIDS[j];//当前向量是Head节点
                                selections[vecOffset].tonode = j;
                                ++postingListSize[selections[vecOffset].node];
                                ++replicaCount[j];
                            }
                        }

                        if (p_opt.m_batches > 1)
                        {
                            if (selections.SaveBatch() != ErrorCode::Success)
                            {
                                return false;
                            }
                        }
                    }
                }
                auto t2 = std::chrono::high_resolution_clock::now();
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Searching replicas ended. Search Time: %.2lf mins\n", ((double)std::chrono::duration_cast<std::chrono::seconds>(t2 - t1).count()) / 60.0);

                if (p_opt.m_batches > 1)
                {
                    if (selections.LoadBatch(0, static_cast<size_t>(fullCount) * p_opt.m_replicaCount) != ErrorCode::Success)
                    {
                        return false;
                    }
                }

                // Sort results either in CPU or GPU
                VectorIndex::SortSelections(&selections.m_selections);//按照HeadID来排序

                auto t3 = std::chrono::high_resolution_clock::now();
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Time to sort selections:%.2lf sec.\n", ((double)std::chrono::duration_cast<std::chrono::seconds>(t3 - t2).count()) + ((double)std::chrono::duration_cast<std::chrono::milliseconds>(t3 - t2).count()) / 1000);

                int postingSizeLimit = INT_MAX;
                if (p_opt.m_postingPageLimit > 0)//人为定义postingPageLimit=3，只允许postinglist存放3个页面。
                {
                    p_opt.m_postingPageLimit = max(p_opt.m_postingPageLimit, static_cast<int>((p_opt.m_postingVectorLimit * vectorInfoSize + PageSize - 1) / PageSize));//m_postingVectorLimit=118，人为设定最少保留的向量个数
                    p_opt.m_searchPostingPageLimit = p_opt.m_postingPageLimit;
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Build index with posting page limit:%d\n", p_opt.m_postingPageLimit);
                    postingSizeLimit = static_cast<int>(p_opt.m_postingPageLimit * PageSize / vectorInfoSize);
                }

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Posting size limit: %d\n", postingSizeLimit);

                {
                    std::vector<int> replicaCountDist(p_opt.m_replicaCount + 1, 0);//创建分布数组
                    for (int i = 0; i < replicaCount.size(); ++i)//replicaCount.size()为全量向量
                    {
                        if (headVectorIDS.count(i) > 0) continue;//排除Head节点，只统计原始数据节点
                        ++replicaCountDist[replicaCount[i]];//replicacount[i]:i向量有多少副本，
                    }

                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Before Posting Cut:\n");
                    for (int i = 0; i < replicaCountDist.size(); ++i)
                    {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Replica Count Dist: %d, %d\n", i, replicaCountDist[i]);
                    }
                }
                {
                    std::vector<std::thread> mythreads;
                    mythreads.reserve(m_opt->m_iSSDNumberOfThreads);
                    std::atomic_size_t sent(0);
                    for (int tid = 0; tid < m_opt->m_iSSDNumberOfThreads; tid++)
                    {
                        mythreads.emplace_back([&, tid]() {
                            size_t i = 0;
                            while (true)
                            {
                                i = sent.fetch_add(1);
                                if (i < postingListSize.size())
                                {
                                    if (postingListSize[i] <= postingSizeLimit)
                                        continue;

                                    std::size_t selectIdx =
                                        std::lower_bound(selections.m_selections.begin(), selections.m_selections.end(),
                                                         i, Selection::g_edgeComparer) -
                                        selections.m_selections.begin();

                                    for (size_t dropID = postingSizeLimit; dropID < postingListSize[i]; ++dropID)
                                    {
                                        int tonode = selections.m_selections[selectIdx + dropID].tonode;//取出被丢弃向量的向量ID
                                        --replicaCount[tonode];
                                    }
                                    postingListSize[i] = postingSizeLimit;//postinglist截断
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
                }
                if (p_opt.m_outputEmptyReplicaID)
                {
                    std::vector<int> replicaCountDist(p_opt.m_replicaCount + 1, 0);
                    auto ptr = SPTAG::f_createIO();
                    if (ptr == nullptr || !ptr->Initialize("EmptyReplicaID.bin", std::ios::binary | std::ios::out)) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to create EmptyReplicaID.bin!\n");
                        return false;
                    }
                    for (int i = 0; i < replicaCount.size(); ++i)
                    {
                        if (headVectorIDS.count(i) > 0) continue;

                        ++replicaCountDist[replicaCount[i]];

                        if (replicaCount[i] < 2)
                        {
                            long long vid = i;
                            if (ptr->WriteBinary(sizeof(vid), reinterpret_cast<char*>(&vid)) != sizeof(vid)) {
                                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failt to write EmptyReplicaID.bin!");
                                return false;
                            }
                        }
                    }

                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "After Posting Cut:\n");
                    for (int i = 0; i < replicaCountDist.size(); ++i)
                    {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Replica Count Dist: %d, %d\n", i, replicaCountDist[i]);
                    }
                }

                auto t4 = std::chrono::high_resolution_clock::now();
                SPTAGLIB_LOG(SPTAG::Helper::LogLevel::LL_Info, "Time to perform posting cut:%.2lf sec.\n", ((double)std::chrono::duration_cast<std::chrono::seconds>(t4 - t3).count()) + ((double)std::chrono::duration_cast<std::chrono::milliseconds>(t4 - t3).count()) / 1000);

                // number of posting lists per file
                size_t postingFileSize = (postingListSize.size() + p_opt.m_ssdIndexFileNum - 1) / p_opt.m_ssdIndexFileNum;//Head总数/要生成几个SSD索引文件=每个SSD文件负责多少个Head（向上取整）,默认是一个。
                std::vector<size_t> selectionsBatchOffset(p_opt.m_ssdIndexFileNum + 1, 0);//第i个SSD索引文件在Selection中对应的起始位置
                for (int i = 0; i < p_opt.m_ssdIndexFileNum; i++) {
                    size_t curPostingListEnd = min(postingListSize.size(), (i + 1) * postingFileSize);
                    selectionsBatchOffset[i + 1] = std::lower_bound(selections.m_selections.begin(), selections.m_selections.end(), (SizeType)curPostingListEnd, Selection::g_edgeComparer) - selections.m_selections.begin();
                }//前i个SSD文件一共用了多少条记录，Selectionbatchoffset=[0,100,210,330,...]

                if (p_opt.m_ssdIndexFileNum > 1)
                {
                    if (selections.SaveBatch() != ErrorCode::Success)
                    {
                        return false;
                    }
                }

                auto fullVectors = p_reader->GetVectorSet();
                if (p_opt.m_distCalcMethod == DistCalcMethod::Cosine && !p_reader->IsNormalized() && !p_headIndex->m_pQuantizer) fullVectors->Normalize(p_opt.m_iSSDNumberOfThreads);

                // iterate over files
                for (int i = 0; i < p_opt.m_ssdIndexFileNum; i++) {
                    size_t curPostingListOffSet = i * postingFileSize;
                    size_t curPostingListEnd = min(postingListSize.size(), (i + 1) * postingFileSize);
                    // postingListSize: number of vectors in the posting list, type vector<int>
                    std::vector<int> curPostingListSizes(
                        postingListSize.begin() + curPostingListOffSet,
                        postingListSize.begin() + curPostingListEnd);//当前SSD文件的postinglist的大小

                    std::vector<size_t> curPostingListBytes(curPostingListSizes.size());//准备用来计算每个postinglist在磁盘上占用的实际字节数
                    
                    if (p_opt.m_ssdIndexFileNum > 1)
                    {
                        if (selections.LoadBatch(selectionsBatchOffset[i], selectionsBatchOffset[i + 1]) != ErrorCode::Success)
                        {
                            return false;
                        }
                    }
                    // create compressor
                    if (p_opt.m_enableDataCompression && i == 0)
                    {
                        m_pCompressor = std::make_unique<Compressor>(p_opt.m_zstdCompressLevel, p_opt.m_dictBufferCapacity);
                        // train dict
                        if (p_opt.m_enableDictTraining) {
                            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Training dictionary...\n");
                            std::string samplesBuffer("");
                            std::vector<size_t> samplesSizes;
                            for (int j = 0; j < curPostingListSizes.size(); j++) {
                                if (curPostingListSizes[j] == 0) {
                                    continue;
                                }
                                ValueType* headVector = nullptr;
                                if (p_opt.m_enableDeltaEncoding)
                                {
                                    headVector = (ValueType*)p_headIndex->GetSample(j);
                                }
                                std::string postingListFullData = GetPostingListFullData(
                                    j, curPostingListSizes[j], selections, fullVectors, p_opt.m_enableDeltaEncoding, p_opt.m_enablePostingListRearrange, headVector);

                                samplesBuffer += postingListFullData;
                                samplesSizes.push_back(postingListFullData.size());
                                if (samplesBuffer.size() > p_opt.m_minDictTraingBufferSize) break;
                            }
                            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Using the first %zu postingLists to train dictionary... \n", samplesSizes.size());
                            std::size_t dictSize = m_pCompressor->TrainDict(samplesBuffer, &samplesSizes[0], (unsigned int)samplesSizes.size());
                            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Dictionary trained, dictionary size: %zu \n", dictSize);
                        }
                    }

                    if (p_opt.m_enableDataCompression) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Getting compressed size of each posting list...\n");
                        std::vector<std::thread> mythreads;
                        mythreads.reserve(m_opt->m_iSSDNumberOfThreads);
                        std::atomic_size_t sent(0);
                        for (int tid = 0; tid < m_opt->m_iSSDNumberOfThreads; tid++)
                        {
                            mythreads.emplace_back([&, tid]() {
                                size_t j = 0;
                                while (true)
                                {
                                    j = sent.fetch_add(1);
                                    if (j < curPostingListSizes.size())
                                    {
                                        SizeType postingListId = j + (SizeType)curPostingListOffSet;
                                        // do not compress if no data
                                        if (postingListSize[postingListId] == 0)
                                        {
                                            curPostingListBytes[j] = 0;
                                            continue;
                                        }
                                        ValueType *headVector = nullptr;
                                        if (p_opt.m_enableDeltaEncoding)
                                        {
                                            headVector = (ValueType *)p_headIndex->GetSample(postingListId);
                                        }
                                        std::string postingListFullData =
                                            GetPostingListFullData(postingListId, postingListSize[postingListId],
                                                                   selections, fullVectors, p_opt.m_enableDeltaEncoding,
                                                                   p_opt.m_enablePostingListRearrange, headVector);
                                        size_t sizeToCompress = postingListSize[postingListId] * vectorInfoSize;
                                        if (sizeToCompress != postingListFullData.size())
                                        {
                                            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                                                         "Size to compress NOT MATCH! PostingListFullData size: %zu "
                                                         "sizeToCompress: %zu \n",
                                                         postingListFullData.size(), sizeToCompress);
                                        }
                                        curPostingListBytes[j] = m_pCompressor->GetCompressedSize(
                                            postingListFullData, p_opt.m_enableDictTraining);
                                        if (postingListId % 10000 == 0 ||
                                            curPostingListBytes[j] >
                                                static_cast<uint64_t>(p_opt.m_postingPageLimit) * PageSize)
                                        {
                                            SPTAGLIB_LOG(
                                                Helper::LogLevel::LL_Info,
                                                "Posting list %d/%d, compressed size: %d, compression ratio: %.4f\n",
                                                postingListId, postingListSize.size(), curPostingListBytes[j],
                                                curPostingListBytes[j] / float(sizeToCompress));
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

                        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Getted compressed size for all the %d posting lists in SSD Index file %d.\n", curPostingListBytes.size(), i);
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Mean compressed size: %.4f \n", std::accumulate(curPostingListBytes.begin(), curPostingListBytes.end(), 0.0) / curPostingListBytes.size());
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Mean compression ratio: %.4f \n", std::accumulate(curPostingListBytes.begin(), curPostingListBytes.end(), 0.0) / (std::accumulate(curPostingListSizes.begin(), curPostingListSizes.end(), 0.0) * vectorInfoSize));
                    }
                    else {
                        for (int j = 0; j < curPostingListSizes.size(); j++)
                        {
                            curPostingListBytes[j] = curPostingListSizes[j] * vectorInfoSize;//第j个Head的postinglist有多少个向量*一个向量占用的字节数=第j个Head的postinglist总共占多少字节
                        }
                    }

                    std::unique_ptr<int[]> postPageNum;
                    std::unique_ptr<std::uint16_t[]> postPageOffset;
                    std::vector<int> postingOrderInIndex;
                    SelectPostingOffset(curPostingListBytes, postPageNum, postPageOffset, postingOrderInIndex);//构建SSD索引时，用来计算每个pl在SSD文件上的物理布局位置。

                    OutputSSDIndexFile((i == 0) ? outputFile : outputFile + "_" + std::to_string(i),
                        p_opt.m_enableDeltaEncoding,
                        p_opt.m_enablePostingListRearrange,
                        p_opt.m_enableDataCompression,
                        p_opt.m_enableDictTraining,
                        vectorInfoSize,
                        curPostingListSizes,
                        curPostingListBytes,
                        p_headIndex,
                        selections,
                        postPageNum,
                        postPageOffset,
                        postingOrderInIndex,
                        fullVectors,
                        curPostingListOffSet);
                }

                p_versionMap.Save(p_opt.m_indexDirectory + FolderSep + p_opt.m_deleteIDFile);

                auto t5 = std::chrono::high_resolution_clock::now();
                auto elapsedSeconds = std::chrono::duration_cast<std::chrono::seconds>(t5 - t1).count();
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Total used time: %.2lf minutes (about %.2lf hours).\n", elapsedSeconds / 60.0, elapsedSeconds / 3600.0);
                return true;
            }

            virtual bool CheckValidPosting(SizeType postingID)
            {
                return m_listInfos[postingID].listEleCount != 0;
            }

            virtual ErrorCode CheckPosting(SizeType postingID, std::vector<bool> *visited = nullptr,
                                           ExtraWorkSpace *p_exWorkSpace = nullptr) override
            {
                if (postingID < 0 || postingID >= m_totalListCount)
                {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "[CheckPosting]: Error postingID %d (should be 0 ~ %d)\n",
                                 postingID, m_totalListCount);
                    return ErrorCode::Key_OverFlow;
                }
                if (m_listInfos[postingID].listEleCount < 0)
                {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "[CheckPosting]: postingID %d has wrong size:%d\n",
                                 postingID, m_listInfos[postingID].listEleCount);
                    return ErrorCode::Posting_SizeError;
                }
                return ErrorCode::Success;
            }

            virtual ErrorCode GetPostingDebug(ExtraWorkSpace* p_exWorkSpace, std::shared_ptr<VectorIndex> p_index, SizeType vid, std::vector<SizeType>& VIDs, std::shared_ptr<VectorSet>& vecs)
            {
                VIDs.clear();

                SizeType curPostingID = vid;
                ListInfo* listInfo = &(m_listInfos[curPostingID]);
                VIDs.resize(listInfo->listEleCount);
                ByteArray vector_array = ByteArray::Alloc(sizeof(ValueType) * listInfo->listEleCount * m_iDataDimension);
                vecs.reset(new BasicVectorSet(vector_array, GetEnumValueType<ValueType>(), m_iDataDimension, listInfo->listEleCount));

                int fileid = m_oneContext ? 0 : curPostingID / m_listPerFile;

#ifndef BATCH_READ
                Helper::DiskIO* indexFile = m_indexFiles[fileid].get();
#endif

                size_t totalBytes = (static_cast<size_t>(listInfo->listPageCount) << PageSizeEx);

#ifdef ASYNC_READ       
                auto& request = p_exWorkSpace->m_diskRequests[0];
                request.m_offset = listInfo->listOffset;
                request.m_readSize = totalBytes;
                request.m_status = (fileid << 16) | (request.m_status & 0xffff);
                request.m_payload = (void*)listInfo;
                request.m_success = false;

#ifdef BATCH_READ // async batch read
                request.m_callback = [&p_exWorkSpace, &vecs, &VIDs, &p_index, &request, this](bool success)
                {
                    char* buffer = request.m_buffer;
                    ListInfo* listInfo = (ListInfo*)(request.m_payload);

                    // decompress posting list
                    char* p_postingListFullData = buffer + listInfo->pageOffset;
                    if (m_enableDataCompression)
                    {
                        DecompressPosting();
                    }

                    for (int i = 0; i < listInfo->listEleCount; i++) 
                    {
                            uint64_t offsetVectorID, offsetVector; 
                            (this->*m_parsePosting)(offsetVectorID, offsetVector, i, listInfo->listEleCount); 
                            int vectorID = *(reinterpret_cast<int*>(p_postingListFullData + offsetVectorID)); 
                            (this->*m_parseEncoding)(p_index, listInfo, (ValueType*)(p_postingListFullData + offsetVector)); 
                            VIDs[i] = vectorID;
                            auto outVec = vecs->GetVector(i);
                            memcpy(outVec, (void*)(p_postingListFullData + offsetVector), sizeof(ValueType) * m_iDataDimension);
                    } 
                };
#else // async read
                request.m_callback = [&p_exWorkSpace, &request](bool success)
                {
                    p_exWorkSpace->m_processIocp.push(&request);
                };

                ++unprocessed;
                if (!(indexFile->ReadFileAsync(request)))
                {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read file!\n");
                    unprocessed--;
                }
#endif
#else // sync read
                char* buffer = (char*)((p_exWorkSpace->m_pageBuffers[0]).GetBuffer());
                auto numRead = indexFile->ReadBinary(totalBytes, buffer, listInfo->listOffset);
                if (numRead != totalBytes) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "File %s read bytes, expected: %zu, acutal: %llu.\n", m_extraFullGraphFile.c_str(), totalBytes, numRead);
                    throw std::runtime_error("File read mismatch");
                }
                // decompress posting list
                char* p_postingListFullData = buffer + listInfo->pageOffset;
                if (m_enableDataCompression)
                {
                    DecompressPosting();
                }

                for (int i = 0; i < listInfo->listEleCount; i++) 
                {
                    uint64_t offsetVectorID, offsetVector;
                    (this->*m_parsePosting)(offsetVectorID, offsetVector, i, listInfo->listEleCount);
                    int vectorID = *(reinterpret_cast<int*>(p_postingListFullData + offsetVectorID));
                    (this->*m_parseEncoding)(p_index, listInfo, (ValueType*)(p_postingListFullData + offsetVector));
                    VIDs[i] = vectorID;
                    auto outVec = vecs->GetVector(i);
                    memcpy(outVec, (void*)(p_postingListFullData + offsetVector), sizeof(ValueType) * m_iDataDimension);
                }
#endif
                return ErrorCode::Success;
            }

        private:
            struct ListInfo
            {
                std::size_t listTotalBytes = 0;
                
                int listEleCount = 0;

                std::uint16_t listPageCount = 0;

                std::uint64_t listOffset = 0;

                std::uint16_t pageOffset = 0;
            };

            int LoadingHeadInfo(const std::string& p_file, int p_postingPageLimit, std::vector<ListInfo>& p_listInfos)//从SSD索引文件中读取pl的头信息，并把这些头信息存到内存结构listinfo中，形成SSD搜索时必须的目录
            {
                auto ptr = SPTAG::f_createIO();
                if (ptr == nullptr || !ptr->Initialize(p_file.c_str(), std::ios::binary | std::ios::in)) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to open file: %s\n", p_file.c_str());
                    throw std::runtime_error("Failed open file in LoadingHeadInfo");
                }
                m_pCompressor = std::make_unique<Compressor>(); // no need compress level to decompress

                int m_listCount;
                int m_totalDocumentCount;
                int m_listPageOffset;

                if (ptr->ReadBinary(sizeof(m_listCount), reinterpret_cast<char*>(&m_listCount)) != sizeof(m_listCount)) {//pl的数量
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                    throw std::runtime_error("Failed read file in LoadingHeadInfo");
                }
                if (ptr->ReadBinary(sizeof(m_totalDocumentCount), reinterpret_cast<char*>(&m_totalDocumentCount)) != sizeof(m_totalDocumentCount)) {//pl中向量的总数量
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                    throw std::runtime_error("Failed read file in LoadingHeadInfo");
                }
                if (ptr->ReadBinary(sizeof(m_iDataDimension), reinterpret_cast<char*>(&m_iDataDimension)) != sizeof(m_iDataDimension)) {//向量维度
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                    throw std::runtime_error("Failed read file in LoadingHeadInfo");
                }
                if (ptr->ReadBinary(sizeof(m_listPageOffset), reinterpret_cast<char*>(&m_listPageOffset)) != sizeof(m_listPageOffset)) {//pl在磁盘文件中的起始页码偏移量
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                    throw std::runtime_error("Failed read file in LoadingHeadInfo");
                }

                if (m_vectorInfoSize == 0) m_vectorInfoSize = m_iDataDimension * sizeof(ValueType) + sizeof(int);//每个向量在磁盘上的空间
                else if (m_vectorInfoSize != m_iDataDimension * sizeof(ValueType) + sizeof(int)) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read head info file! DataDimension and ValueType are not match!\n");
                    throw std::runtime_error("DataDimension and ValueType don't match in LoadingHeadInfo");
                }

                size_t totalListCount = p_listInfos.size();
                p_listInfos.resize(totalListCount + m_listCount);

                size_t totalListElementCount = 0;

                std::map<int, int> pageCountDist;

                size_t biglistCount = 0;
                size_t biglistElementCount = 0;
                int pageNum;
                for (int i = 0; i < m_listCount; ++i)//逐个读取pl的头信息
                {
                    ListInfo* listInfo = &(p_listInfos[totalListCount + i]);//pl的头信息存放在listInfo结构中

                    if (m_enableDataCompression)
                    {
                        if (ptr->ReadBinary(sizeof(listInfo->listTotalBytes), reinterpret_cast<char*>(&(listInfo->listTotalBytes))) != sizeof(listInfo->listTotalBytes)) {
                            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                            throw std::runtime_error("Failed read file in LoadingHeadInfo");
                        }
                    }
                    if (ptr->ReadBinary(sizeof(pageNum), reinterpret_cast<char*>(&(pageNum))) != sizeof(pageNum)) {//读取的都是Build阶段写入文件的原始目录信息
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                        throw std::runtime_error("Failed read file in LoadingHeadInfo");
                    }
                    if (ptr->ReadBinary(sizeof(listInfo->pageOffset), reinterpret_cast<char*>(&(listInfo->pageOffset))) != sizeof(listInfo->pageOffset)) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                        throw std::runtime_error("Failed read file in LoadingHeadInfo");
                    }
                    if (ptr->ReadBinary(sizeof(listInfo->listEleCount), reinterpret_cast<char*>(&(listInfo->listEleCount))) != sizeof(listInfo->listEleCount)) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                        throw std::runtime_error("Failed read file in LoadingHeadInfo");
                    }
                    if (ptr->ReadBinary(sizeof(listInfo->listPageCount), reinterpret_cast<char*>(&(listInfo->listPageCount))) != sizeof(listInfo->listPageCount)) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                        throw std::runtime_error("Failed read file in LoadingHeadInfo");
                    }
                    listInfo->listOffset = (static_cast<uint64_t>(m_listPageOffset + pageNum) << PageSizeEx);//计算pl在磁盘文件中的实际起始物理地址
                    if (!m_enableDataCompression)
                    {
                        listInfo->listTotalBytes = listInfo->listEleCount * m_vectorInfoSize;
                        listInfo->listEleCount = min(listInfo->listEleCount, (min(static_cast<int>(listInfo->listPageCount), p_postingPageLimit) << PageSizeEx) / m_vectorInfoSize);
                        listInfo->listPageCount = static_cast<std::uint16_t>(ceil((m_vectorInfoSize * listInfo->listEleCount + listInfo->pageOffset) * 1.0 / (1 << PageSizeEx)));
                    }
                    totalListElementCount += listInfo->listEleCount;
                    int pageCount = listInfo->listPageCount;

                    if (pageCount > 1)
                    {
                        ++biglistCount;
                        biglistElementCount += listInfo->listEleCount;
                    }

                    if (pageCountDist.count(pageCount) == 0)//统计占N页的pl有多少个
                    {
                        pageCountDist[pageCount] = 1;
                    }
                    else
                    {
                        pageCountDist[pageCount] += 1;
                    }
                }

                if (m_enableDataCompression && m_enableDictTraining)
                {
                    size_t dictBufferSize;
                    if (ptr->ReadBinary(sizeof(size_t), reinterpret_cast<char*>(&dictBufferSize)) != sizeof(dictBufferSize)) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                        throw std::runtime_error("Failed read file in LoadingHeadInfo");
                    }
                    char* dictBuffer = new char[dictBufferSize];
                    if (ptr->ReadBinary(dictBufferSize, dictBuffer) != dictBufferSize) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read head info file!\n");
                        throw std::runtime_error("Failed read file in LoadingHeadInfo");
                    }
                    try {
                        m_pCompressor->SetDictBuffer(std::string(dictBuffer, dictBufferSize));
                    }
                    catch (std::runtime_error& err) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read head info file: %s \n", err.what());
                        throw std::runtime_error("Failed read file in LoadingHeadInfo");
                    }
                    delete[] dictBuffer;
                }

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                    "Finish reading header info, list count %d, total doc count %d, dimension %d, list page offset %d.\n",
                    m_listCount,
                    m_totalDocumentCount,
                    m_iDataDimension,
                    m_listPageOffset);

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                    "Big page (>4K): list count %zu, total element count %zu.\n",
                    biglistCount,
                    biglistElementCount);//统计占用页数大于4K的pl数量和这些pl包含的向量总数量

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Total Element Count: %llu\n", totalListElementCount);

                for (auto& ele : pageCountDist)
                {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Page Count Dist: %d %d\n", ele.first, ele.second);
                }

                return m_listCount;
            }

            inline void ParsePostingListRearrange(uint64_t& offsetVectorID, uint64_t& offsetVector, int i, int eleCount)
            {
                offsetVectorID = (m_vectorInfoSize - sizeof(int)) * eleCount + sizeof(int) * i;
                offsetVector = (m_vectorInfoSize - sizeof(int)) * i;
            }

            inline void ParsePostingList(uint64_t& offsetVectorID, uint64_t& offsetVector, int i, int eleCount)
            {
                offsetVectorID = m_vectorInfoSize * i;
                offsetVector = offsetVectorID + sizeof(int);
            }

            inline void ParseDeltaEncoding(std::shared_ptr<VectorIndex>& p_index, ListInfo* p_info, ValueType* vector)
            {
                ValueType* headVector = (ValueType*)p_index->GetSample((SizeType)(p_info - m_listInfos.data()));
                COMMON::SIMDUtils::ComputeSum(vector, headVector, m_iDataDimension);
            }

            inline void ParseEncoding(std::shared_ptr<VectorIndex>& p_index, ListInfo* p_info, ValueType* vector) { }

            void SelectPostingOffset(
                const std::vector<size_t>& p_postingListBytes,
                std::unique_ptr<int[]>& p_postPageNum,
                std::unique_ptr<std::uint16_t[]>& p_postPageOffset,
                std::vector<int>& p_postingOrderInIndex)//postinglist写入SSD的物理顺序
            {
                p_postPageNum.reset(new int[p_postingListBytes.size()]);
                p_postPageOffset.reset(new std::uint16_t[p_postingListBytes.size()]);

                struct PageModWithID
                {
                    int id;//第几个postinglist

                    std::uint16_t rest;//这个postinglist的多余
                };

                struct PageModeWithIDCmp
                {
                    bool operator()(const PageModWithID& a, const PageModWithID& b) const
                    {
                        return a.rest == b.rest ? a.id < b.id : a.rest > b.rest;//多余相同，用ID排序
                    }
                };

                std::set<PageModWithID, PageModeWithIDCmp> listRestSize;

                p_postingOrderInIndex.clear();
                p_postingOrderInIndex.reserve(p_postingListBytes.size());

                PageModWithID listInfo;
                for (size_t i = 0; i < p_postingListBytes.size(); ++i)//遍历每一个postinglist
                {
                    if (p_postingListBytes[i] == 0)
                    {
                        continue;
                    }

                    listInfo.id = static_cast<int>(i);
                    listInfo.rest = static_cast<std::uint16_t>(p_postingListBytes[i] % PageSize);

                    listRestSize.insert(listInfo);//所有postinglist多余进入集合
                }

                listInfo.id = -1;

                int currPageNum = 0;
                std::uint16_t currOffset = 0;//当前Page已经写了多少字节

                while (!listRestSize.empty())
                {
                    listInfo.rest = PageSize - currOffset;//当前Page还剩多少空间，用它作为能不能放进去的判断条件
                    auto iter = listRestSize.lower_bound(listInfo); // avoid page-crossing，在listRestSize中找一个Rest小于当前剩余空间的。
                    if (iter == listRestSize.end() || (listInfo.rest != PageSize && iter->rest == 0))
                    {
                        ++currPageNum;
                        currOffset = 0;
                    }
                    else//找到了一个可以放进当前Page的多余
                    {
                        p_postPageNum[iter->id] = currPageNum;//记录第ID个postinglist从第currpagenum页开始
                        p_postPageOffset[iter->id] = currOffset;//记录在该Page内的起始字节偏移

                        p_postingOrderInIndex.push_back(iter->id);//记录这个postinglist的物理写入顺序

                        currOffset += iter->rest;//把这个postinglist的多余的放进Page，指针后移
                        if (currOffset > PageSize)
                        {
                            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Crossing extra pages\n");
                            throw std::runtime_error("Read too many pages"); 
                        }

                        if (currOffset == PageSize)//如果刚好填满一页，直接换到下一页
                        {
                            ++currPageNum;
                            currOffset = 0;
                        }

                        currPageNum += static_cast<int>(p_postingListBytes[iter->id] / PageSize);//跳过这个postinglist的完整页部分

                        listRestSize.erase(iter);
                    }
                }

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "TotalPageNumbers: %d, IndexSize: %llu\n", currPageNum, static_cast<uint64_t>(currPageNum) * PageSize + currOffset);
            }

            void OutputSSDIndexFile(const std::string& p_outputFile,
                bool p_enableDeltaEncoding,
                bool p_enablePostingListRearrange,
                bool p_enableDataCompression,
                bool p_enableDictTraining,
                size_t p_spacePerVector,
                const std::vector<int>& p_postingListSizes,
                const std::vector<size_t>& p_postingListBytes,
                std::shared_ptr<VectorIndex> p_headIndex,
                Selection& p_postingSelections,
                const std::unique_ptr<int[]>& p_postPageNum,
                const std::unique_ptr<std::uint16_t[]>& p_postPageOffset,
                const std::vector<int>& p_postingOrderInIndex,
                std::shared_ptr<VectorSet> p_fullVectors,
                size_t p_postingListOffset)
            {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Start output...\n");

                auto t1 = std::chrono::high_resolution_clock::now();

                auto ptr = SPTAG::f_createIO();//创建io句柄
                int retry = 3;
                // open file 
                while (retry > 0 && (ptr == nullptr || !ptr->Initialize(p_outputFile.c_str(), std::ios::binary | std::ios::out)))
                {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed open file %s, retrying...\n", p_outputFile.c_str());
                    retry--;
                }

                if (ptr == nullptr || !ptr->Initialize(p_outputFile.c_str(), std::ios::binary | std::ios::out)) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed open file %s\n", p_outputFile.c_str());
                    throw std::runtime_error("Failed to open file for SSD index save");
                }
                // meta size of global info
                std::uint64_t listOffset = sizeof(int) * 4;//基础的全局信息
                // meta size of the posting lists
                listOffset += (sizeof(int) + sizeof(std::uint16_t) + sizeof(int) + sizeof(std::uint16_t)) * p_postingListSizes.size();//加上每个pl的索引信息，向量数，页号，页偏移，
                // write listTotalBytes only when enabled data compression
                if (p_enableDataCompression)
                {
                    listOffset += sizeof(size_t) * p_postingListSizes.size();
                }

                // compression dict
                if (p_enableDataCompression && p_enableDictTraining)
                {
                    listOffset += sizeof(size_t);
                    listOffset += m_pCompressor->GetDictBuffer().size();
                }

                std::unique_ptr<char[]> paddingVals(new char[PageSize]);
                memset(paddingVals.get(), 0, sizeof(char) * PageSize);//准备一个4KB的全0 缓冲区用于填充
                // paddingSize: bytes left in the last page
                std::uint64_t paddingSize = PageSize - (listOffset % PageSize);//postinglist内容一定从整个Page开始。 4-（10%4）=2
                if (paddingSize == PageSize)
                {
                    paddingSize = 0;
                }
                else
                {
                    listOffset += paddingSize;//更新偏移量，将填充空间计入，10+2=12，目录用了一半页，还有一半用0填充，正文新起一页开始。
                }

                // Number of posting lists
                int i32Val = static_cast<int>(p_postingListSizes.size());
                if (ptr->WriteBinary(sizeof(i32Val), reinterpret_cast<char*>(&i32Val)) != sizeof(i32Val)) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }

                // Number of vectors
                i32Val = static_cast<int>(p_fullVectors->Count());
                if (ptr->WriteBinary(sizeof(i32Val), reinterpret_cast<char*>(&i32Val)) != sizeof(i32Val)) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }

                // Vector dimension
                i32Val = static_cast<int>(p_fullVectors->Dimension());
                if (ptr->WriteBinary(sizeof(i32Val), reinterpret_cast<char*>(&i32Val)) != sizeof(i32Val)) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }

                // Page offset of list content section
                i32Val = static_cast<int>(listOffset / PageSize);//12/4=3,从第4页开始都是正文
                if (ptr->WriteBinary(sizeof(i32Val), reinterpret_cast<char*>(&i32Val)) != sizeof(i32Val)) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }

                // Meta of each posting list
                for (int i = 0; i < p_postingListSizes.size(); ++i)
                {
                    size_t postingListByte = 0;//该列表的实际占用字节数
                    int pageNum = 0; // starting page number
                    std::uint16_t pageOffset = 0;//该列表在起始页内的字节偏移
                    int listEleCount = 0;//列表里一共有多少个向量
                    std::uint16_t listPageCount = 0;//这个列表一共跨越了多少个Page

                    if (p_postingListSizes[i] > 0)
                    {
                        pageNum = p_postPageNum[i];
                        pageOffset = static_cast<std::uint16_t>(p_postPageOffset[i]);
                        listEleCount = static_cast<int>(p_postingListSizes[i]);
                        postingListByte = p_postingListBytes[i];
                        listPageCount = static_cast<std::uint16_t>(postingListByte / PageSize);//跨页的数量
                        if (0 != (postingListByte % PageSize))
                        {
                            ++listPageCount;
                        }
                    }
                    // Total bytes of the posting list, write only when enabled data compression
                    if (p_enableDataCompression && ptr->WriteBinary(sizeof(postingListByte), reinterpret_cast<char*>(&postingListByte)) != sizeof(postingListByte)) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                        throw std::runtime_error("Failed to write SSDIndex File");
                    }
                    // Page number of the posting list
                    if (ptr->WriteBinary(sizeof(pageNum), reinterpret_cast<char*>(&pageNum)) != sizeof(pageNum)) {//写入页面编号
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                        throw std::runtime_error("Failed to write SSDIndex File");
                    }
                    // Page offset
                    if (ptr->WriteBinary(sizeof(pageOffset), reinterpret_cast<char*>(&pageOffset)) != sizeof(pageOffset)) {//写入页面偏移
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                        throw std::runtime_error("Failed to write SSDIndex File");
                    }
                    // Number of vectors in the posting list
                    if (ptr->WriteBinary(sizeof(listEleCount), reinterpret_cast<char*>(&listEleCount)) != sizeof(listEleCount)) {//写入该pl的向量数
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                        throw std::runtime_error("Failed to write SSDIndex File");
                    }
                    // Page count of the posting list
                    if (ptr->WriteBinary(sizeof(listPageCount), reinterpret_cast<char*>(&listPageCount)) != sizeof(listPageCount)) {//写入该pl占用的页数
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                        throw std::runtime_error("Failed to write SSDIndex File");
                    }
                }
                // compression dict
                if (p_enableDataCompression && p_enableDictTraining)
                {
                    std::string dictBuffer = m_pCompressor->GetDictBuffer();
                    // dict size
                    size_t dictBufferSize = dictBuffer.size();
                    if (ptr->WriteBinary(sizeof(size_t), reinterpret_cast<char *>(&dictBufferSize)) != sizeof(dictBufferSize))
                    {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                        throw std::runtime_error("Failed to write SSDIndex File");
                    }
                    // dict
                    if (ptr->WriteBinary(dictBuffer.size(), const_cast<char *>(dictBuffer.data())) != dictBuffer.size())
                    {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                        throw std::runtime_error("Failed to write SSDIndex File");
                    }
                }

                // Write padding vals
                if (paddingSize > 0)
                {
                    if (ptr->WriteBinary(paddingSize, reinterpret_cast<char*>(paddingVals.get())) != paddingSize) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                        throw std::runtime_error("Failed to write SSDIndex File");
                    }
                }

                if (static_cast<uint64_t>(ptr->TellP()) != listOffset)
                {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "List offset not match!\n");
                    throw std::runtime_error("List offset mismatch");
                }

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "SubIndex Size: %llu bytes, %llu MBytes\n", listOffset, listOffset >> 20);

                listOffset = 0;

                std::uint64_t paddedSize = 0;
                // iterate over all the posting lists
                for (auto id : p_postingOrderInIndex)//按物理写入顺序，SelectPostingoffset计算了每个pl的位置，现在写真实数据
                {
                    std::uint64_t targetOffset = static_cast<uint64_t>(p_postPageNum[id]) * PageSize + p_postPageOffset[id];//这个pl应该写在哪个页，哪个偏移
                    if (targetOffset < listOffset)
                    {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "List offset not match, targetOffset < listOffset!\n");
                        throw std::runtime_error("List offset mismatch");
                    }
                    // write padding vals before the posting list
                    if (targetOffset > listOffset)// listoffset 空洞 targetoffset，
                    {
                        if (targetOffset - listOffset > PageSize)
                        {
                            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Padding size greater than page size!\n");
                            throw std::runtime_error("Padding size mismatch with page size");
                        }

                        if (ptr->WriteBinary(targetOffset - listOffset, reinterpret_cast<char*>(paddingVals.get())) != targetOffset - listOffset) {
                            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                            throw std::runtime_error("Failed to write SSDIndex File");
                        }

                        paddedSize += targetOffset - listOffset;

                        listOffset = targetOffset;//跳过空洞
                    }

                    if (p_postingListSizes[id] == 0)//跳过空的postinglist
                    {
                        continue;
                    }
                    int postingListId = id + (int)p_postingListOffset;//计算得到该postinglist的实际ID
                    // get posting list full content and write it at once
                    ValueType *headVector = nullptr;
                    if (p_enableDeltaEncoding)
                    {
                        headVector = (ValueType *)p_headIndex->GetSample(postingListId);
                    }
                    std::string postingListFullData = GetPostingListFullData(
                        postingListId, p_postingListSizes[id], p_postingSelections, p_fullVectors, p_enableDeltaEncoding, p_enablePostingListRearrange, headVector);
                    size_t postingListFullSize = p_postingListSizes[id] * p_spacePerVector;//计算出该postinglist需要占用的字节数
                    if (postingListFullSize != postingListFullData.size())//对比计算出的大小与获取出的数据大小
                    {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "posting list full data size NOT MATCH! postingListFullData.size(): %zu postingListFullSize: %zu \n", postingListFullData.size(), postingListFullSize);
                        throw std::runtime_error("Posting list full size mismatch");
                    }
                    if (p_enableDataCompression)
                    {
                        std::string compressedData = m_pCompressor->Compress(postingListFullData, p_enableDictTraining);
                        size_t compressedSize = compressedData.size();
                        if (compressedSize != p_postingListBytes[id])
                        {
                            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Compressed size NOT MATCH! compressed size:%zu, pre-calculated compressed size:%zu\n", compressedSize, p_postingListBytes[id]);
                            throw std::runtime_error("Compression size mismatch");
                        }
                        if (ptr->WriteBinary(compressedSize, compressedData.data()) != compressedSize)
                        {
                            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                            throw std::runtime_error("Failed to write SSDIndex File");
                        }
                        listOffset += compressedSize;
                    }
                    else
                    {
                        if (ptr->WriteBinary(postingListFullSize, postingListFullData.data()) != postingListFullSize)//把整个pl的内容，按字节，连续写进SSDIndex文件
                        {
                            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                            throw std::runtime_error("Failed to write SSDIndex File");
                        }
                        listOffset += postingListFullSize;
                    }
                }

                paddingSize = PageSize - (listOffset % PageSize);//当前文件是否需要填充
                if (paddingSize == PageSize)
                {
                    paddingSize = 0;
                }
                else
                {
                    listOffset += paddingSize;
                    paddedSize += paddingSize;
                }

                if (paddingSize > 0)
                {
                    if (ptr->WriteBinary(paddingSize, reinterpret_cast<char *>(paddingVals.get())) != paddingSize)//向SSDindex文件中写入paddingSize字节，内容是paddingvals
                    {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to write SSDIndex File!");
                        throw std::runtime_error("Failed to write SSDIndex File");
                    }
                }

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Padded Size: %llu, final total size: %llu.\n", paddedSize, listOffset);

                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Output done...\n");
                auto t2 = std::chrono::high_resolution_clock::now();
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Time to write results:%.2lf sec.\n", ((double)std::chrono::duration_cast<std::chrono::seconds>(t2 - t1).count()) + ((double)std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count()) / 1000);
            }

            ErrorCode GetWritePosting(ExtraWorkSpace* p_exWorkSpace, SizeType pid, std::string& posting, bool write = false) override {
                if (write) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Unsupport write\n");
                    return ErrorCode::Undefined;
                }
                ListInfo* listInfo = &(m_listInfos[pid]);
                size_t totalBytes = (static_cast<size_t>(listInfo->listPageCount) << PageSizeEx);
                size_t realBytes = listInfo->listEleCount * m_vectorInfoSize;
                posting.resize(totalBytes);
                int fileid = m_oneContext? 0: pid / m_listPerFile;
                Helper::DiskIO* indexFile = m_indexFiles[fileid].get();
                auto numRead = indexFile->ReadBinary(totalBytes, (char*)posting.data(), listInfo->listOffset);
                if (numRead != totalBytes) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "File %s read bytes, expected: %zu, acutal: %llu.\n", m_extraFullGraphFile.c_str(), totalBytes, numRead);
                    return ErrorCode::DiskIOFail;
                }
                char* ptr = (char*)(posting.c_str());
                memcpy(ptr, posting.c_str() + listInfo->pageOffset, realBytes);
                posting.resize(realBytes);
                return ErrorCode::Success;
            }

        private:
            bool m_available = false;

            std::shared_ptr<Helper::Concurrent::ConcurrentQueue<int>> m_freeWorkSpaceIds;
            std::atomic<int> m_workspaceCount = 0;

            std::string m_extraFullGraphFile;

            std::vector<ListInfo> m_listInfos;
            bool m_oneContext;
            Options* m_opt;

            std::vector<std::shared_ptr<Helper::DiskIO>> m_indexFiles;
            std::unique_ptr<Compressor> m_pCompressor;
            bool m_enableDeltaEncoding;
            bool m_enablePostingListRearrange;
            bool m_enableDataCompression;
            bool m_enableDictTraining;
            
            void (ExtraStaticSearcher<ValueType>::*m_parsePosting)(uint64_t&, uint64_t&, int, int);
            void (ExtraStaticSearcher<ValueType>::*m_parseEncoding)(std::shared_ptr<VectorIndex>&, ListInfo*, ValueType*);

            int m_vectorInfoSize = 0;
            int m_iDataDimension = 0;

            int m_totalListCount = 0;

            int m_listPerFile = 0;
        };
    } // namespace SPANN
} // namespace SPTAG

#endif // _SPTAG_SPANN_EXTRASTATICSEARCHER_H_
