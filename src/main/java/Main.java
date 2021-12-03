import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource;

public class Main {

	public static void main(String[] args) throws Exception {
        final String connectionString = Files.readString(Path.of("connection-string.txt"));
        final Path tempPath = Path.of("target", "checkout");
        // final Path tempPath = Path.of("C:\\Dev\\src\\campus\\mpo\\mpo-ui\\.git");
        final String repoName = "mpo-ui";

        if (Files.notExists(tempPath)) {
            throw new RuntimeException("The path " + tempPath + " does not exist.");
        }

        CommitDatabase database = connectToDatabase(connectionString);

        exportRepo(tempPath, repoName, database);
    }

    private static CommitDatabase connectToDatabase(String connectionString) {

        // DataSource dataSource = new SimpleDriverDataSource();
        SQLServerConnectionPoolDataSource dataSource = new SQLServerConnectionPoolDataSource();
        dataSource.setURL(connectionString);

        JdbcCommitDatabase database = new JdbcCommitDatabase(dataSource);
        database.pingDatabase();
        return database;
    }

    private static void exportRepo(final Path tempPath, final String repoName, CommitDatabase database) throws Exception {
        Repository repository = openRepository(tempPath);
		
        long snapshotId = database.createSnapshot(repoName);

        System.out.println("Created snapshot " + snapshotId);

        saveBranches(database, repoName, snapshotId, findBranchesSortedByDate(repository));
        
        saveReleases(repository, database, repoName);

        // saveCommits(repository, database, repoName);
    }

    static void saveCommits(Repository repo, CommitDatabase database, String repoName) throws Exception {
        List<Ref> branches = findBranchesSortedByDate(repo);
        List<Ref> releases = findReleasesSortedByDate(repo);
        List<RevCommit> alreadyHandledCommits = new ArrayList<>();

        try (RevWalk walk = new RevWalk(repo)) {
            saveCommits(database, repoName, releases, alreadyHandledCommits, walk, true);
            saveCommits(database, repoName, branches, alreadyHandledCommits, walk, false);
        }
    }

    private static void saveCommits(CommitDatabase database, String repoName, List<Ref> roots, List<RevCommit> alreadyHandledCommits, RevWalk walk,
            boolean rootsAreReleases)
            throws IOException {
        ArrayList<Ref> rootsToProcess = new ArrayList<>(roots);

        while (!rootsToProcess.isEmpty()) {
            final Ref root = rootsToProcess.get(0);

            // System.out.println(root.getName());

            final RevCommit currentCommit = walk.parseCommit(root.getObjectId());

            final Optional<Long> currentCommitTimestamp = findCommitTimestamp(currentCommit);

            walk.markStart(currentCommit);

            for (RevCommit alreadyHandledCommit : alreadyHandledCommits) {
                walk.markUninteresting(alreadyHandledCommit);
            }

            RevCommit commit;
            long count = 0;

            while ((commit = walk.next()) != null) {
                Set<String> parents = Arrays.stream(commit.getParents()).map(parent -> parent.getName()).collect(Collectors.toSet());
                database.saveCommit(repoName,
                        commit.getName(),
                        commit.getAuthorIdent().getWhen().getTime(),
                        root.getName(),
                        rootsAreReleases ? currentCommitTimestamp : null,
                        parents);
                ++count;
            }

            rootsToProcess.remove(0);
            alreadyHandledCommits.add(currentCommit);
            walk.reset();

            System.out.println("Saved " + count + " commits");
        }
    }

    static Optional<Long> findRefTimestamp(Repository repo, Ref ref) {
        try {
            return findCommitTimestamp(repo.parseCommit(ref.getObjectId()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Optional<Long> findCommitTimestamp(RevCommit commit) {
        try {
            return Optional.of(commit.getAuthorIdent().getWhen().getTime());
        } catch (NullPointerException e) {
            // Il y a un bug avec le commit 31f3d296eb1d0d50ed056c01c54dd60ea7bf62d7 dans mpo-ui
            // java.lang.NullPointerException at org.eclipse.jgit.util.RawParseUtils.author(RawParseUtils.java:726)
            return Optional.empty();
        }
    }

    static Repository openRepository(Path path) throws IOException {
        FileRepositoryBuilder builder = new FileRepositoryBuilder();
        return builder.setGitDir(path.toFile())
                .build();
    }

    static void saveBranches(CommitDatabase database, String repoName, long snapshotId, List<Ref> branches) throws Exception {
        database.saveBranches(repoName,
                snapshotId,
                branches.stream()
                        .map(ref -> new SimpleRef(ref.getName(), Optional.empty(), ref.getObjectId().getName()))
                        .toList());
    }

    static void saveReleases(Repository repository, CommitDatabase database, String repoName) throws Exception {
        List<Ref> releases = findReleasesSortedByDate(repository);

        database.saveReleases(repoName,
                releases.stream()
                        .map(ref -> new SimpleRef(ref.getName(),
                                findRefTimestamp(repository, ref),
                                ref.getObjectId().getName()))
                        .toList());
    }

    static List<Ref> findReleasesSortedByDate(Repository repository) throws Exception {
        return findTagsSortedByDate(repository).stream()
                .filter(ref -> ref.getName().matches(".+@[\\d\\.]+$"))
                .collect(Collectors.toList());
    }

    static List<Ref> findBranchesSortedByDate(Repository repository) throws Exception {
        return repository.getRefDatabase().getRefsByPrefix(Constants.R_HEADS).stream()
                .sorted(new SortByCommitDateComparator(repository))
                .collect(Collectors.toList());
	}

    static List<Ref> findTagsSortedByDate(Repository repository) throws Exception {
        return repository.getRefDatabase().getRefsByPrefix(Constants.R_TAGS).stream()
                .sorted(new SortByCommitDateComparator(repository))
                .collect(Collectors.toList());
    }

    static class SortByCommitDateComparator implements Comparator<Ref> {
        private final Repository repo;

        SortByCommitDateComparator(Repository repo) {
            this.repo = repo;
        }

        @Override
        public int compare(Ref ref1, Ref ref2) {
            return findDate(ref1).compareTo(findDate(ref2));
        }

        Date findDate(Ref ref) {
            try {
                return repo.parseCommit(ref.getObjectId()).getAuthorIdent().getWhen();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    interface CommitDatabase {
        long createSnapshot(String repoName);

        void saveCommit(String repoName, String hash, long commitTimestamp, String releaseName, Optional<Long> releaseTimestamp, Set<String> parents);

        void saveBranches(String repoName, long snapshotId, Collection<? extends SimpleRef> branches);

        void saveBranch(String repoName, long snapshotId, String branchName, String commitHash);

        void saveReleases(String repoName, Collection<? extends SimpleRef> releases);

        void saveRelease(String repoName, String releaseName, long releaseTimestamp, String commitHash);
    }

    static class DummyCommitDatabase implements CommitDatabase {
        @Override
        public long createSnapshot(String repoName) {
            return 1;
        }

        @Override
        public void saveCommit(String repoName, String hash, long commitTimestamp, String releaseName, Optional<Long> releaseTimestamp, Set<String> parents) {

            System.out.println("Commit " + hash + ": " + releaseName);
        }

        @Override
        public void saveBranches(String repoName, long snapshotId, Collection<? extends SimpleRef> branches) {

        }

        @Override
        public void saveBranch(String repoName, long snapshotId, String branchName, String commitHash) {

        }

        @Override
        public void saveReleases(String repoName, Collection<? extends SimpleRef> releases) {

        }

        @Override
        public void saveRelease(String repoName, String releaseName, long releaseTimestamp, String commitHash) {

        }
    }

    static class JdbcCommitDatabase implements CommitDatabase {
        private final JdbcTemplate jdbcTemplate;

        public JdbcCommitDatabase(DataSource dataSource) {
            this.jdbcTemplate = new JdbcTemplate(dataSource);
        }

        public void pingDatabase() {
            jdbcTemplate.queryForObject("select 1", Integer.class);
        }

        @Override
        public long createSnapshot(String repoName) {
            SimpleJdbcInsert insert = new SimpleJdbcInsert(jdbcTemplate);
            insert.setGeneratedKeyName("Snapshot_Id");
            insert.setColumnNames(Arrays.asList("Repo_Name", "Snapshot_Timestamp"));
            insert.setTableName("Snapshots");
            Number generatedId = insert.executeAndReturnKey(new MapSqlParameterSource()
                    .addValue("Repo_Name", repoName)
                    .addValue("Snapshot_Timestamp", new Date()));

            return generatedId.longValue();
        }

        @Override
        public void saveCommit(String repoName, String hash, long commitTimestamp, String releaseName, Optional<Long> releaseTimestamp, Set<String> parents) {

        }

        @Override
        public void saveBranches(String repoName, long snapshotId, Collection<? extends SimpleRef> branches) {
            jdbcTemplate.batchUpdate("insert into Branches(Repo_Name, Snapshot_Id, Branch_Name, Commit_Hash) values(?, ?, ?, ?)",
                    branches.stream()
                            .map(ref -> new Object[] { repoName, snapshotId, ref.name, ref.commitHash })
                            .toList());
        }

        @Override
        public void saveBranch(String repoName, long snapshotId, String branchName, String commitHash) {
            jdbcTemplate.update("insert into Branches(Repo_Name, Snapshot_Id, Branch_Name, Commit_Hash) values(?, ?, ?, ?)",
                    repoName, snapshotId, branchName, commitHash);
        }

        @Override
        public void saveReleases(String repoName, Collection<? extends SimpleRef> releases) {
            jdbcTemplate.batchUpdate("insert into Releases(Repo_Name, Release_Name, Release_Timestamp, Commit_Hash) values(?, ?, ?, ?)",
                    releases.stream()
                            .map(ref -> new Object[] { repoName, ref.name, new Date(ref.timestamp.get()), ref.commitHash })
                            .toList());
        }

        @Override
        public void saveRelease(String repoName, String releaseName, long releaseTimestamp, String commitHash) {
            jdbcTemplate.update("insert into Releases(Repo_Name, Release_Name, Release_Timestamp, Commit_Hash) values(?, ?, ?, ?)",
                    repoName, releaseName, new Date(releaseTimestamp), commitHash);
        }
    }

    static class SimpleRef {
        final String name;
        final Optional<Long> timestamp;
        final String commitHash;

        SimpleRef(String name, Optional<Long> timestamp, String commitHash) {
            this.name = name;
            this.timestamp = timestamp;
            this.commitHash = commitHash;
        }
    }
}
