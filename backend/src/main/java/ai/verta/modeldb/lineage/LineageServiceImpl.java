package ai.verta.modeldb.lineage;

import ai.verta.modeldb.AddLineage;
import ai.verta.modeldb.DeleteLineage;
import ai.verta.modeldb.FindAllInputs;
import ai.verta.modeldb.FindAllInputsOutputs;
import ai.verta.modeldb.FindAllOutputs;
import ai.verta.modeldb.LineageEntry;
import ai.verta.modeldb.LineageServiceGrpc.LineageServiceImplBase;
import ai.verta.modeldb.Location;
import ai.verta.modeldb.ModelDBAuthInterceptor;
import ai.verta.modeldb.ModelDBException;
import ai.verta.modeldb.VersioningLineageEntry;
import ai.verta.modeldb.entities.versioning.InternalFolderElementEntity;
import ai.verta.modeldb.entities.versioning.RepositoryEntity;
import ai.verta.modeldb.experimentRun.ExperimentRunDAO;
import ai.verta.modeldb.monitoring.QPSCountResource;
import ai.verta.modeldb.monitoring.RequestLatencyResource;
import ai.verta.modeldb.utils.ModelDBUtils;
import ai.verta.modeldb.versioning.BlobDAO;
import ai.verta.modeldb.versioning.CommitDAO;
import ai.verta.modeldb.versioning.GetCommitComponentRequest.Response;
import ai.verta.modeldb.versioning.RepositoryDAO;
import ai.verta.modeldb.versioning.RepositoryIdentification;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;

public class LineageServiceImpl extends LineageServiceImplBase {

  private static final Logger LOGGER = LogManager.getLogger(LineageServiceImpl.class);
  private final ExperimentRunDAO experimentDAO;
  private final RepositoryDAO repositoryDAO;
  private final CommitDAO commitDAO;
  private final LineageDAO lineageDAO;
  private final BlobDAO blobDAO;

  public LineageServiceImpl(
      LineageDAO lineageDAO,
      ExperimentRunDAO experimentRunDAO,
      RepositoryDAO repositoryDAO,
      CommitDAO commitDAO,
      BlobDAO blobDAO) {
    this.lineageDAO = lineageDAO;
    this.experimentDAO = experimentRunDAO;
    this.repositoryDAO = repositoryDAO;
    this.commitDAO = commitDAO;
    this.blobDAO = blobDAO;
  }

  @Override
  public void addLineage(AddLineage request, StreamObserver<AddLineage.Response> responseObserver) {
    QPSCountResource.inc();
    try {
      if (request.getInputCount() == 0 && request.getOutputCount() == 0) {
        throw new ModelDBException("Input and output not specified", Code.INVALID_ARGUMENT);
      } else {
        if (request.getInputCount() == 0) {
          throw new ModelDBException("Input not specified", Code.INVALID_ARGUMENT);
        } else if (request.getOutputCount() == 0) {
          throw new ModelDBException("Output not specified", Code.INVALID_ARGUMENT);
        }
      }
      try (RequestLatencyResource latencyResource =
          new RequestLatencyResource(ModelDBAuthInterceptor.METHOD_NAME.get())) {
        AddLineage.Response response =
            lineageDAO.addLineage(request, this::checkResourcesExists, this::getBlobFromCommit);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      }
    } catch (Exception e) {
      ModelDBUtils.observeError(responseObserver, e, AddLineage.Response.getDefaultInstance());
    }
  }

  @Override
  public void deleteLineage(
      DeleteLineage request, StreamObserver<DeleteLineage.Response> responseObserver) {
    QPSCountResource.inc();
    try {
      try (RequestLatencyResource latencyResource =
          new RequestLatencyResource(ModelDBAuthInterceptor.METHOD_NAME.get())) {
        DeleteLineage.Response response =
            lineageDAO.deleteLineage(request, this::getBlobFromCommit);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      }
    } catch (Exception e) {
      ModelDBUtils.observeError(responseObserver, e, DeleteLineage.Response.getDefaultInstance());
    }
  }

  @Override
  public void findAllInputs(
      FindAllInputs request, StreamObserver<FindAllInputs.Response> responseObserver) {
    QPSCountResource.inc();
    try {
      if (request.getItemsCount() == 0) {
        throw new ModelDBException("Items not specified", Code.INVALID_ARGUMENT);
      }
      try (RequestLatencyResource latencyResource =
          new RequestLatencyResource(ModelDBAuthInterceptor.METHOD_NAME.get())) {
        FindAllInputs.Response response =
            lineageDAO.findAllInputs(request, this::getBlobFromCommit, this::getCommitFromBlob);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      }
    } catch (Exception e) {
      ModelDBUtils.observeError(responseObserver, e, FindAllInputs.Response.getDefaultInstance());
    }
  }

  @Override
  public void findAllOutputs(
      FindAllOutputs request, StreamObserver<FindAllOutputs.Response> responseObserver) {
    QPSCountResource.inc();
    try {
      if (request.getItemsCount() == 0) {
        throw new ModelDBException("Items not specified", Code.INVALID_ARGUMENT);
      }
      try (RequestLatencyResource latencyResource =
          new RequestLatencyResource(ModelDBAuthInterceptor.METHOD_NAME.get())) {
        FindAllOutputs.Response response =
            lineageDAO.findAllOutputs(request, this::getBlobFromCommit, this::getCommitFromBlob);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      }
    } catch (Exception e) {
      ModelDBUtils.observeError(responseObserver, e, FindAllOutputs.Response.getDefaultInstance());
    }
  }

  @Override
  public void findAllInputsOutputs(
      FindAllInputsOutputs request,
      StreamObserver<FindAllInputsOutputs.Response> responseObserver) {
    QPSCountResource.inc();
    try {
      if (request.getItemsCount() == 0) {
        throw new ModelDBException("Items not specified", Code.INVALID_ARGUMENT);
      }
      try (RequestLatencyResource latencyResource =
          new RequestLatencyResource(ModelDBAuthInterceptor.METHOD_NAME.get())) {
        FindAllInputsOutputs.Response response =
            lineageDAO.findAllInputsOutputs(
                request, this::getBlobFromCommit, this::getCommitFromBlob);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      }
    } catch (Exception e) {
      ModelDBUtils.observeError(
          responseObserver, e, FindAllInputsOutputs.Response.getDefaultInstance());
    }
  }

  private InternalFolderElementEntity getBlobFromCommit(
      Session session, VersioningLineageEntry versioningLineageEntry) throws ModelDBException {
    Entry<Response, InternalFolderElementEntity> result =
        blobDAO.getCommitComponentWithHash(
            session1 -> getRepositoryById(session, versioningLineageEntry.getRepositoryId()),
            versioningLineageEntry.getCommitSha(),
            versioningLineageEntry.getLocationList());
    return result.getValue();
  }

  private RepositoryEntity getRepositoryById(Session session, long repositoryId) {
    try {
      return repositoryDAO.getRepositoryById(
          session, RepositoryIdentification.newBuilder().setRepoId(repositoryId).build());
    } catch (ModelDBException e) {
      LOGGER.warn("repository not found {}", e.getMessage());
      return null;
    }
  }

  private VersioningLineageEntry getCommitFromBlob(
      Session session, VersioningBlobEntryContainer versioningBlobElement) throws ModelDBException {
    return blobDAO.getVersioningEntryByBlob(
        session,
        session1 -> getRepositoryById(session, versioningBlobElement.getRepositoryId()),
        versioningBlobElement);
  }

  private void checkResourcesExists(Session session, List<LineageEntry> lineageEntries)
      throws ModelDBException, NoSuchAlgorithmException, InvalidProtocolBufferException {
    Set<String> experimentRuns = new HashSet<>();
    Map<Long, Map.Entry<RepositoryEntity, Map<String, Set<String>>>> blobs = new HashMap<>();
    for (LineageEntry lineageEntry : lineageEntries) {
      switch (lineageEntry.getDescriptionCase()) {
        case EXPERIMENT_RUN:
          String experimentRun = lineageEntry.getExperimentRun();
          if (!experimentRuns.contains(experimentRun)) {
            boolean result = experimentDAO.isExperimentRunExists(session, experimentRun);
            if (!result) {
              throw new ModelDBException("Experiment run doesn't exist");
            }
            experimentRuns.add(experimentRun);
          }
          break;
        case BLOB:
          VersioningLineageEntry blob = lineageEntry.getBlob();
          long repositoryId = blob.getRepositoryId();
          RepositoryEntity repo;
          Map<String, Set<String>> result;
          if (!blobs.containsKey(repositoryId)) {
            repo =
                repositoryDAO.getRepositoryById(
                    session, RepositoryIdentification.newBuilder().setRepoId(repositoryId).build());
            blobs.put(repositoryId, new AbstractMap.SimpleEntry<>(repo, new HashMap<>()));
            result = blobs.get(repositoryId).getValue();
          } else {
            Entry<RepositoryEntity, Map<String, Set<String>>> entityMapEntry =
                blobs.get(repositoryId);
            repo = entityMapEntry.getKey();
            result = entityMapEntry.getValue();
          }
          String commitSha = blob.getCommitSha();
          Set<String> blobResult;
          if (!result.containsKey(commitSha)) {
            commitDAO.getCommitEntity(session, commitSha, session1 -> repo);
            result.put(commitSha, new HashSet<>());
          }
          blobResult = result.get(commitSha);
          String stringFromProtoObject =
              ModelDBUtils.getStringFromProtoObject(
                  Location.newBuilder().addAllLocation(blob.getLocationList()));
          if (!blobResult.contains(stringFromProtoObject)) {
            blobDAO.getCommitComponent(session1 -> repo, commitSha, blob.getLocationList());
            Entry<Response, InternalFolderElementEntity> blobHashInfo =
                blobDAO.getCommitComponentWithHash(
                    session1 -> repo, commitSha, blob.getLocationList());
            String elementSha = blobHashInfo.getValue().getElement_sha();
            if (elementSha == null || elementSha.isEmpty()) {
              throw new ModelDBException("Wrong blob specified", Code.INVALID_ARGUMENT);
            }
            blobResult.add(stringFromProtoObject);
          }
          break;
        default:
          throw new ModelDBException("Unexpected type", Code.INTERNAL);
      }
    }
  }
}
