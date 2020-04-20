import jetbrains.buildServer.configs.kotlin.v2019_2.*
import jetbrains.buildServer.configs.kotlin.v2019_2.buildSteps.gradle
import jetbrains.buildServer.configs.kotlin.v2019_2.triggers.vcs
import jetbrains.buildServer.configs.kotlin.v2019_2.vcs.GitVcsRoot

/*
The settings script is an entry point for defining a TeamCity
project hierarchy. The script should contain a single call to the
project() function with a Project instance or an init function as
an argument.

VcsRoots, BuildTypes, Templates, and subprojects can be
registered inside the project using the vcsRoot(), buildType(),
template(), and subProject() methods respectively.

To debug settings scripts in command-line, run the

    mvnDebug org.jetbrains.teamcity:teamcity-configs-maven-plugin:generate

command and attach your debugger to the port 8000.

To debug in IntelliJ Idea, open the 'Maven Projects' tool window (View
-> Tool Windows -> Maven Projects), find the generate task node
(Plugins -> teamcity-configs -> teamcity-configs:generate), the
'Debug' option is available in the context menu for the task.
*/

version = "2019.2"

project {

    vcsRoot(HttpsGithubComWemVertxKinesisConsumerOrchestraRefsHeadsMaster)

    buildType(Build)
    buildType(BuildAndUpload)
}

object Build : BuildType({
    name = "Test only"

    allowExternalStatus = true

    vcs {
        root(DslContext.settingsRoot)
    }

    steps {
        gradle {
            tasks = "clean test"
            buildFile = ""
            coverageEngine = idea {
                includeClasses = "ch.sourcemotion.*"
            }
        }
    }

    triggers {
        vcs {
        }
    }
})

object BuildAndUpload : BuildType({
    name = "Build and upload"

    params {
        text("system.release_version", "", label = "Release version", display = ParameterDisplay.PROMPT, allowEmpty = false)
    }

    vcs {
        root(HttpsGithubComWemVertxKinesisConsumerOrchestraRefsHeadsMaster)
    }

    steps {
        gradle {
            tasks = "clean build bintrayUpload"
            buildFile = ""
        }
    }
})

object HttpsGithubComWemVertxKinesisConsumerOrchestraRefsHeadsMaster : GitVcsRoot({
    name = "https://github.com/wem/vertx-kinesis-consumer-orchestra#refs/heads/master"
    url = "https://github.com/wem/vertx-kinesis-consumer-orchestra"
    authMethod = password {
        userName = "wem"
        password = "credentialsJSON:071f192b-1525-4b10-866a-99f15b39f864"
    }
})
