/*
 * Copyright (c) 2015 LingoChamp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.liulishuo.filedownloader.database;

import android.content.Context;
import android.text.TextUtils;
import android.util.SparseArray;

import com.liulishuo.filedownloader.model.ConnectionModel;
import com.liulishuo.filedownloader.model.ConnectionModel_;
import com.liulishuo.filedownloader.model.FileDownloadModel;
import com.liulishuo.filedownloader.model.FileDownloadModel_;
import com.liulishuo.filedownloader.model.FileDownloadStatus;
import com.liulishuo.filedownloader.model.MyObjectBox;
import com.liulishuo.filedownloader.util.FileDownloadHelper;
import com.liulishuo.filedownloader.util.FileDownloadLog;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import io.objectbox.Box;
import io.objectbox.BoxStore;

public class ObjectBoxDBImpl implements FileDownloadDatabase {
    private static BoxStore boxStore;

    public static void init(Context context) {
        boxStore = MyObjectBox.builder()
                .androidContext(context.getApplicationContext())
                .build();
    }

    public static BoxStore get() {
        return boxStore;
    }

    public ObjectBoxDBImpl() {
        if (boxStore == null)
            init(FileDownloadHelper.getAppContext());
    }

    @Override
    public void onTaskStart(int id) {

    }

    @Override
    public FileDownloadModel find(int id) {
        return getFileDownloadModelBox()
                .query()
                .equal(FileDownloadModel_.id, id)
                .build()
                .findFirst();
    }

    @Override
    public List<ConnectionModel> findConnectionModel(int id) {
        return get()
                .boxFor(ConnectionModel.class)
                .query()
                .equal(ConnectionModel_.id, id)
                .build()
                .find();
    }

    @Override
    public void removeConnections(int id) {
        get()
                .boxFor(ConnectionModel.class)
                .query()
                .equal(ConnectionModel_.id, id)
                .build()
                .remove();
    }

    @Override
    public void insertConnectionModel(ConnectionModel model) {
        get()
                .boxFor(ConnectionModel.class)
                .put(model);
    }

    @Override
    public void updateConnectionModel(int id, int index, long currentOffset) {
        final ConnectionModel connectionModel = get()
                .boxFor(ConnectionModel.class)
                .query()
                .equal(ConnectionModel_.id, id)
                .equal(ConnectionModel_.index, index)
                .build()
                .findFirst();
        if (connectionModel != null) {

            connectionModel.setCurrentOffset(currentOffset);
            get()
                    .boxFor(ConnectionModel.class)
                    .put(connectionModel);
        }

    }

    @Override
    public void updateConnectionCount(int id, int count) {
        final FileDownloadModel fileDownloadModel = getFileDownloadModelBox()
                .query()
                .equal(FileDownloadModel_.id, id)
                .build()
                .findFirst();
        if (fileDownloadModel != null) {
            fileDownloadModel.setConnectionCount(count);
            getFileDownloadModelBox().put(fileDownloadModel);
        }
    }

    @Override
    public void insert(FileDownloadModel downloadModel) {
        getFileDownloadModelBox()
                .put(downloadModel);
    }

    @Override
    public void update(FileDownloadModel downloadModel) {
        if (downloadModel == null) {
            FileDownloadLog.w(this, "update but model == null!");
            return;
        }
        getFileDownloadModelBox()
                .put(downloadModel);
    }

    @Override
    public boolean remove(int id) {
        getFileDownloadModelBox()
                .query()
                .equal(FileDownloadModel_.id, id)
                .build()
                .remove();
        return false;
    }

    @Override
    public void clear() {
        getFileDownloadModelBox().removeAll();
        get().boxFor(ConnectionModel.class).removeAll();
    }

    @Override
    public void updateOldEtagOverdue(int id, String newEtag, long sofar, long total,
                                     int connectionCount) {
        final FileDownloadModel fileDownloadModel = getFileDownloadModelById(id);
        if (fileDownloadModel != null) {
            fileDownloadModel.setETag(newEtag);
            fileDownloadModel.setSoFar(sofar);
            fileDownloadModel.setTotal(total);
            fileDownloadModel.setConnectionCount(connectionCount);
            getFileDownloadModelBox().put(fileDownloadModel);
        }

    }

    @Override
    public void updateConnected(int id, long total, String etag, String filename) {
        final FileDownloadModel fileDownloadModel = getFileDownloadModelById(id);
        if (fileDownloadModel != null) {
            fileDownloadModel.setETag(etag);
            fileDownloadModel.setTotal(total);
            fileDownloadModel.setStatus(FileDownloadStatus.connected);
            fileDownloadModel.setFilename(filename);
            getFileDownloadModelBox().put(fileDownloadModel);
        }
    }

    private FileDownloadModel getFileDownloadModelById(int id) {
        return getFileDownloadModelBox()
                .query()
                .equal(FileDownloadModel_.id, id)
                .build()
                .findUnique();
    }

    private Box<FileDownloadModel> getFileDownloadModelBox() {
        return get()
                .boxFor(FileDownloadModel.class);
    }

    @Override
    public void updateProgress(int id, long sofarBytes) {
        final FileDownloadModel fileDownloadModel = getFileDownloadModelById(id);
        if (fileDownloadModel != null) {
            fileDownloadModel.setStatus(FileDownloadStatus.progress);
            fileDownloadModel.setSoFar(sofarBytes);
            getFileDownloadModelBox().put(fileDownloadModel);
        }
    }

    @Override
    public void updateError(int id, Throwable throwable, long sofar) {
        final FileDownloadModel fileDownloadModel = getFileDownloadModelById(id);
        if (fileDownloadModel != null) {
            fileDownloadModel.setStatus(FileDownloadStatus.error);
            fileDownloadModel.setSoFar(sofar);
            fileDownloadModel.setErrMsg(throwable.toString());
            getFileDownloadModelBox().put(fileDownloadModel);
        }
    }

    @Override
    public void updateRetry(int id, Throwable throwable) {
        final FileDownloadModel fileDownloadModel = getFileDownloadModelById(id);
        if (fileDownloadModel != null) {
            fileDownloadModel.setStatus(FileDownloadStatus.retry);
            fileDownloadModel.setErrMsg(throwable.toString());
            getFileDownloadModelBox().put(fileDownloadModel);
        }
    }

    @Override
    public void updateCompleted(int id, final long total) {
        final FileDownloadModel fileDownloadModel = getFileDownloadModelById(id);
        if (fileDownloadModel != null) {
            fileDownloadModel.setStatus(FileDownloadStatus.completed);
            fileDownloadModel.setSoFar(total);
            getFileDownloadModelBox().put(fileDownloadModel);
        }
    }

    @Override
    public void updatePause(int id, long sofar) {
        final FileDownloadModel fileDownloadModel = getFileDownloadModelById(id);
        if (fileDownloadModel != null) {
            fileDownloadModel.setStatus(FileDownloadStatus.paused);
            fileDownloadModel.setSoFar(sofar);
            getFileDownloadModelBox().put(fileDownloadModel);
        }
    }

    @Override
    public void updatePending(int id) {
        // No need to persist pending status.
        final FileDownloadModel fileDownloadModel = getFileDownloadModelById(id);
        if (fileDownloadModel != null) {
            fileDownloadModel.setStatus(FileDownloadStatus.pending);
            getFileDownloadModelBox().put(fileDownloadModel);
        }
    }

    @Override
    public FileDownloadDatabase.Maintainer maintainer() {
        return new Maintainer();
    }

    public FileDownloadDatabase.Maintainer maintainer(
            SparseArray<FileDownloadModel> downloaderModelMap,
            SparseArray<List<ConnectionModel>> connectionModelListMap) {
        return new Maintainer(downloaderModelMap, connectionModelListMap);
    }

    public class Maintainer implements FileDownloadDatabase.Maintainer {

        private final SparseArray<FileDownloadModel> needChangeIdList = new SparseArray<>();
        private MaintainerIterator currentIterator;

        private final SparseArray<FileDownloadModel> downloaderModelMap;
        private final SparseArray<List<ConnectionModel>> connectionModelListMap;

        Maintainer() {
            this(null, null);
        }

        Maintainer(SparseArray<FileDownloadModel> downloaderModelMap,
                   SparseArray<List<ConnectionModel>> connectionModelListMap) {
            this.downloaderModelMap = downloaderModelMap;
            this.connectionModelListMap = connectionModelListMap;
        }

        @Override
        public Iterator<FileDownloadModel> iterator() {
            return currentIterator = new MaintainerIterator();
        }

        @Override
        public void onFinishMaintain() {
            if (currentIterator != null) currentIterator.onFinishMaintain();

            final int length = needChangeIdList.size();
            if (length < 0) return;

            final Box<FileDownloadModel> box = getFileDownloadModelBox();
            for (int i = 0; i < length; i++) {
                final int oldId = needChangeIdList.keyAt(i);
                final FileDownloadModel modelWithNewId = needChangeIdList.get(oldId);
                remove(oldId);
                box.put(modelWithNewId);

                if (modelWithNewId.getConnectionCount() > 1) {
                    List<ConnectionModel> connectionModelList = findConnectionModel(oldId);
                    if (connectionModelList.size() <= 0) continue;
                    removeConnections(oldId);
                    final Box<ConnectionModel> connectionModelBox = get().boxFor(ConnectionModel.class);
                    connectionModelBox.put(connectionModelList);
                }
            }

            // initial cache of connection model
            if (downloaderModelMap != null && connectionModelListMap != null) {
                final int size = downloaderModelMap.size();
                for (int i = 0; i < size; i++) {
                    final int id = downloaderModelMap.valueAt(i).getId();
                    final List<ConnectionModel> connectionModelList = findConnectionModel(id);

                    if (connectionModelList != null && connectionModelList.size() > 0) {
                        connectionModelListMap.put(id, connectionModelList);
                    }
                }
            }
        }

        @Override
        public void onRemovedInvalidData(FileDownloadModel model) {
        }

        @Override
        public void onRefreshedValidData(FileDownloadModel model) {
            if (downloaderModelMap != null) downloaderModelMap.put(model.getId(), model);
        }

        @Override
        public void changeFileDownloadModelId(int oldId, FileDownloadModel modelWithNewId) {
            needChangeIdList.put(oldId, modelWithNewId);
        }

    }


    public static class Maker implements FileDownloadHelper.DatabaseCustomMaker {

        @Override
        public FileDownloadDatabase customMake() {
            return new ObjectBoxDBImpl();
        }
    }

    private class MaintainerIterator implements Iterator<FileDownloadModel> {
        private long currentId;
        private final Iterator<FileDownloadModel> iterator;
        private final List<Long> needRemoveId = new ArrayList<>();

        public MaintainerIterator() {
            final List<FileDownloadModel> fileDownloadModels = getFileDownloadModelBox().query().build().find();
            iterator = fileDownloadModels.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public FileDownloadModel next() {
            final FileDownloadModel model = iterator.next();
            currentId = model.boxId;
            return model;
        }

        @Override
        public void remove() {
            needRemoveId.add(currentId);
        }

        public void onFinishMaintain() {

            if (!needRemoveId.isEmpty()) {
                String args = TextUtils.join(", ", needRemoveId);
                if (FileDownloadLog.NEED_LOG) {
                    FileDownloadLog.d(this, "delete %s", args);
                }
                getFileDownloadModelBox().removeByKeys(needRemoveId);
            }
        }
    }
}
