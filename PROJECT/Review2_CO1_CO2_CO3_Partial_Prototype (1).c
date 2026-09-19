#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define CHILD_COUNT 3

typedef enum {
    EVENT_READY = 1,
    EVENT_STATUS,
    EVENT_WORK,
    EVENT_CONTINUED,
    EVENT_EXITING
} event_type_t;

typedef struct {
    pid_t pid;
    int child_number;
    int event_type;
    int work_count;
} child_event_t;

typedef struct {
    pid_t pid;
    int alive;
} child_info_t;

static child_info_t children[CHILD_COUNT];
static pid_t child_pgid = -1;

static volatile sig_atomic_t parent_got_sigchld = 0;
static volatile sig_atomic_t parent_shutdown = 0;

static volatile sig_atomic_t child_got_usr1 = 0;
static volatile sig_atomic_t child_got_usr2 = 0;
static volatile sig_atomic_t child_got_cont = 0;
static volatile sig_atomic_t child_got_term = 0;

static void child_signal_handler(int signal_number)
{
    if (signal_number == SIGUSR1) {
        child_got_usr1 = 1;
    } else if (signal_number == SIGUSR2) {
        child_got_usr2 = 1;
    } else if (signal_number == SIGCONT) {
        child_got_cont = 1;
    } else if (signal_number == SIGTERM) {
        child_got_term = 1;
    }
}

static void parent_signal_handler(int signal_number)
{
    if (signal_number == SIGCHLD) {
        parent_got_sigchld = 1;
    } else if (signal_number == SIGINT || signal_number == SIGTERM) {
        parent_shutdown = 1;
    }
}

static int install_handler(int signal_number, void (*handler)(int), int flags)
{
    struct sigaction action;

    memset(&action, 0, sizeof(action));
    action.sa_handler = handler;
    action.sa_flags = flags;
    sigemptyset(&action.sa_mask);

    return sigaction(signal_number, &action, NULL);
}

static void send_event(int event_fd, int child_number,
                       event_type_t type, int work_count)
{
    child_event_t event;

    event.pid = getpid();
    event.child_number = child_number;
    event.event_type = type;
    event.work_count = work_count;

    if (write(event_fd, &event, sizeof(event)) == -1) {
        perror("child write");
    }
}

static void run_child(int child_number, int event_fd, pid_t target_pgid)
{
    sigset_t control_signals;
    sigset_t previous_mask;
    int work_count = 0;

    if (target_pgid == 0) {
        if (setpgid(0, 0) == -1 && errno != EACCES) {
            perror("child setpgid");
            _exit(EXIT_FAILURE);
        }
    } else if (setpgid(0, target_pgid) == -1 && errno != EACCES) {
        perror("child setpgid");
        _exit(EXIT_FAILURE);
    }

    sigemptyset(&control_signals);
    sigaddset(&control_signals, SIGUSR1);
    sigaddset(&control_signals, SIGUSR2);
    sigaddset(&control_signals, SIGCONT);
    sigaddset(&control_signals, SIGTERM);

    if (sigprocmask(SIG_BLOCK, &control_signals, &previous_mask) == -1) {
        perror("child sigprocmask");
        _exit(EXIT_FAILURE);
    }

    if (install_handler(SIGUSR1, child_signal_handler, SA_RESTART) == -1 ||
        install_handler(SIGUSR2, child_signal_handler, SA_RESTART) == -1 ||
        install_handler(SIGCONT, child_signal_handler, SA_RESTART) == -1 ||
        install_handler(SIGTERM, child_signal_handler, SA_RESTART) == -1) {
        perror("child sigaction");
        _exit(EXIT_FAILURE);
    }

    signal(SIGINT, SIG_IGN);
    send_event(event_fd, child_number, EVENT_READY, work_count);

    for (;;) {
        while (!child_got_usr1 && !child_got_usr2 &&
               !child_got_cont && !child_got_term) {
            sigsuspend(&previous_mask);
        }

        if (child_got_cont) {
            child_got_cont = 0;
            send_event(event_fd, child_number, EVENT_CONTINUED, work_count);
        }

        if (child_got_usr1) {
            child_got_usr1 = 0;
            send_event(event_fd, child_number, EVENT_STATUS, work_count);
        }

        if (child_got_usr2) {
            child_got_usr2 = 0;
            work_count++;
            send_event(event_fd, child_number, EVENT_WORK, work_count);
        }

        if (child_got_term) {
            send_event(event_fd, child_number, EVENT_EXITING, work_count);
            close(event_fd);
            _exit(EXIT_SUCCESS);
        }
    }
}

static const char *event_name(int event_type)
{
    switch (event_type) {
        case EVENT_READY:
            return "READY";
        case EVENT_STATUS:
            return "STATUS RESPONSE";
        case EVENT_WORK:
            return "WORK EVENT";
        case EVENT_CONTINUED:
            return "CONTINUED";
        case EVENT_EXITING:
            return "EXITING";
        default:
            return "UNKNOWN";
    }
}

static void drain_child_events(int event_fd)
{
    child_event_t event;
    ssize_t bytes_read;

    for (;;) {
        bytes_read = read(event_fd, &event, sizeof(event));
        if (bytes_read == (ssize_t)sizeof(event)) {
            printf("[event] child=%d pid=%ld type=%s work_count=%d\n",
                   event.child_number, (long)event.pid,
                   event_name(event.event_type), event.work_count);
        } else if (bytes_read == -1 && errno == EINTR) {
            continue;
        } else if (bytes_read == -1 &&
                   (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return;
        } else {
            return;
        }
    }
}

static int child_index_from_pid(pid_t pid)
{
    int i;

    for (i = 0; i < CHILD_COUNT; i++) {
        if (children[i].pid == pid) {
            return i;
        }
    }
    return -1;
}

static int alive_children(void)
{
    int i;
    int count = 0;

    for (i = 0; i < CHILD_COUNT; i++) {
        count += children[i].alive;
    }
    return count;
}

static void reap_child_changes(void)
{
    int status;
    pid_t pid;

    for (;;) {
        pid = waitpid(-1, &status, WNOHANG | WUNTRACED | WCONTINUED);
        if (pid <= 0) {
            break;
        }

        int index = child_index_from_pid(pid);
        int child_number = index >= 0 ? index + 1 : 0;

        if (WIFSTOPPED(status)) {
            printf("[state] child=%d pid=%ld STOPPED by signal %d\n",
                   child_number, (long)pid, WSTOPSIG(status));
        } else if (WIFCONTINUED(status)) {
            printf("[state] child=%d pid=%ld CONTINUED\n",
                   child_number, (long)pid);
        } else if (WIFEXITED(status)) {
            printf("[state] child=%d pid=%ld EXITED code=%d\n",
                   child_number, (long)pid, WEXITSTATUS(status));
            if (index >= 0) {
                children[index].alive = 0;
            }
        } else if (WIFSIGNALED(status)) {
            printf("[state] child=%d pid=%ld TERMINATED by signal %d\n",
                   child_number, (long)pid, WTERMSIG(status));
            if (index >= 0) {
                children[index].alive = 0;
            }
        }
    }
}

static void print_proc_state(pid_t pid)
{
    char path[64];
    char line[256];
    FILE *file;

    snprintf(path, sizeof(path), "/proc/%ld/status", (long)pid);
    file = fopen(path, "r");
    if (file == NULL) {
        printf("/proc state unavailable");
        return;
    }

    while (fgets(line, sizeof(line), file) != NULL) {
        if (strncmp(line, "State:", 6) == 0) {
            line[strcspn(line, "\n")] = '\0';
            printf("%s", line);
            fclose(file);
            return;
        }
    }

    fclose(file);
    printf("State not found");
}

static void list_children(void)
{
    int i;

    puts("\nTracked children and kernel-provided /proc state:");
    for (i = 0; i < CHILD_COUNT; i++) {
        printf("  child=%d pid=%ld alive=%s ",
               i + 1, (long)children[i].pid,
               children[i].alive ? "yes" : "no");
        print_proc_state(children[i].pid);
        putchar('\n');
    }
}

static int command_signal(const char *action)
{
    if (strcmp(action, "status") == 0) {
        return SIGUSR1;
    }
    if (strcmp(action, "work") == 0) {
        return SIGUSR2;
    }
    if (strcmp(action, "stop") == 0) {
        return SIGSTOP;
    }
    if (strcmp(action, "cont") == 0) {
        return SIGCONT;
    }
    return 0;
}

static int handle_command(char *line)
{
    char action[32];
    char target[32];
    int signal_number;

    if (sscanf(line, "%31s", action) != 1) {
        return 1;
    }

    if (strcmp(action, "help") == 0) {
        puts("Commands: status|work|stop|cont <1|2|3|all>, list, quit");
        return 1;
    }

    if (strcmp(action, "list") == 0) {
        list_children();
        return 1;
    }

    if (strcmp(action, "quit") == 0) {
        return 0;
    }

    signal_number = command_signal(action);
    if (signal_number == 0 || sscanf(line, "%*s %31s", target) != 1) {
        puts("Invalid command. Type help.");
        return 1;
    }

    if (strcmp(target, "all") == 0) {
        if (kill(-child_pgid, signal_number) == -1) {
            perror("kill process group");
        } else {
            printf("[parent] sent %s to process group %ld\n",
                   action, (long)child_pgid);
        }
        return 1;
    }

    char *end = NULL;
    long child_number = strtol(target, &end, 10);
    if (*target == '\0' || *end != '\0' ||
        child_number < 1 || child_number > CHILD_COUNT) {
        puts("Child number must be 1, 2, 3, or all.");
        return 1;
    }

    int index = (int)child_number - 1;
    if (!children[index].alive) {
        puts("That child has already exited.");
    } else if (kill(children[index].pid, signal_number) == -1) {
        perror("kill child");
    } else {
        printf("[parent] sent %s to child %ld\n",
               action, child_number);
    }

    return 1;
}

static void terminate_children(void)
{
    int status;
    pid_t pid;

    if (child_pgid > 0 && alive_children() > 0) {
        kill(-child_pgid, SIGCONT);
        kill(-child_pgid, SIGTERM);
    }

    while ((pid = waitpid(-1, &status, 0)) > 0 || errno == EINTR) {
        if (pid > 0) {
            int index = child_index_from_pid(pid);
            if (index >= 0) {
                children[index].alive = 0;
            }
            printf("[cleanup] reaped pid=%ld\n", (long)pid);
        }
    }
}

int main(void)
{
    int event_pipe[2];
    int i;

    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stdin, NULL, _IONBF, 0);

    if (pipe(event_pipe) == -1) {
        perror("pipe");
        return EXIT_FAILURE;
    }

    for (i = 0; i < CHILD_COUNT; i++) {
        pid_t target_pgid = child_pgid < 0 ? 0 : child_pgid;
        pid_t pid = fork();

        if (pid == -1) {
            perror("fork");
            parent_shutdown = 1;
            break;
        }

        if (pid == 0) {
            close(event_pipe[0]);
            run_child(i + 1, event_pipe[1], target_pgid);
        }

        children[i].pid = pid;
        children[i].alive = 1;

        if (child_pgid == -1) {
            child_pgid = pid;
        }

        if (setpgid(pid, child_pgid) == -1 &&
            errno != EACCES && errno != ESRCH) {
            perror("parent setpgid");
            parent_shutdown = 1;
            break;
        }
    }

    close(event_pipe[1]);

    int flags = fcntl(event_pipe[0], F_GETFL);
    if (flags == -1 ||
        fcntl(event_pipe[0], F_SETFL, flags | O_NONBLOCK) == -1) {
        perror("fcntl");
        parent_shutdown = 1;
    }

    if (install_handler(SIGCHLD, parent_signal_handler, 0) == -1 ||
        install_handler(SIGINT, parent_signal_handler, 0) == -1 ||
        install_handler(SIGTERM, parent_signal_handler, 0) == -1) {
        perror("parent sigaction");
        parent_shutdown = 1;
    }

    printf("Linux Signal Control System: Review-2 partial prototype\n");
    printf("Parent PID=%ld, child process group=%ld\n",
           (long)getpid(), (long)child_pgid);
    puts("Commands: status|work|stop|cont <1|2|3|all>, list, quit");

    while (!parent_shutdown && alive_children() > 0) {
        struct pollfd inputs[2];
        int ready;

        inputs[0].fd = STDIN_FILENO;
        inputs[0].events = POLLIN;
        inputs[0].revents = 0;
        inputs[1].fd = event_pipe[0];
        inputs[1].events = POLLIN;
        inputs[1].revents = 0;

        ready = poll(inputs, 2, -1);
        if (ready == -1 && errno != EINTR) {
            perror("poll");
            break;
        }

        if (parent_got_sigchld) {
            parent_got_sigchld = 0;
            reap_child_changes();
        }

        if (ready > 0 && (inputs[1].revents & (POLLIN | POLLHUP))) {
            drain_child_events(event_pipe[0]);
        }

        if (ready > 0 && (inputs[0].revents & POLLIN)) {
            char line[128];
            if (fgets(line, sizeof(line), stdin) == NULL ||
                !handle_command(line)) {
                break;
            }
        } else if (ready > 0 && (inputs[0].revents & POLLHUP)) {
            break;
        }
    }

    puts("[parent] starting graceful cleanup");
    terminate_children();
    drain_child_events(event_pipe[0]);
    close(event_pipe[0]);
    puts("[parent] all children reaped; prototype finished");

    return EXIT_SUCCESS;
}
