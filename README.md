# **2025.01.10 : 	PASS lab1**

脚本TIMEOUT修改成120s才通过了，不知道为什么跑的这么慢。后面想办法再改吧。

```
*** Starting wc test.
2025/01/10 17:12:31 Worker 6626 starting map task 0 on file ../pg-being_ernest.txt
2025/01/10 17:12:31 Worker 6629 starting map task 2 on file ../pg-frankenstein.txt
2025/01/10 17:12:31 Worker 6630 starting map task 1 on file ../pg-dorian_gray.txt
2025/01/10 17:12:40 Worker 6626 starting map task 3 on file ../pg-grimm.txt
2025/01/10 17:13:00 Worker 6629 starting map task 4 on file ../pg-huckleberry_finn.txt
2025/01/10 17:13:02 Worker 6630 starting map task 5 on file ../pg-metamorphosis.txt
2025/01/10 17:13:11 Worker 6630 starting map task 6 on file ../pg-sherlock_holmes.txt
2025/01/10 17:13:18 Worker 6626 starting map task 7 on file ../pg-tom_sawyer.txt
2025/01/10 17:13:48 Worker 6630 starting reduce task 0
2025/01/10 17:13:49 Worker 6629 starting reduce task 1
2025/01/10 17:13:49 Worker 6626 starting reduce task 2
2025/01/10 17:13:49 Worker 6630 starting reduce task 3
2025/01/10 17:13:49 Worker 6629 starting reduce task 4
2025/01/10 17:13:50 Worker 6630 starting reduce task 5
2025/01/10 17:13:51 Worker 6626 starting reduce task 6
2025/01/10 17:13:51 Worker 6629 starting reduce task 7
2025/01/10 17:13:51 Worker 6630 starting reduce task 8
2025/01/10 17:13:52 Worker 6629 starting reduce task 9
2025/01/10 17:13:53 Worker 6629 exiting
2025/01/10 17:13:53 Worker 6626 exiting
2025/01/10 17:13:53 Worker 6630 exiting
--- wc test: PASS
*** Starting indexer test.
2025/01/10 17:14:09 Worker 6714 starting map task 0 on file ../pg-being_ernest.txt
2025/01/10 17:14:09 Worker 6715 starting map task 1 on file ../pg-dorian_gray.txt
2025/01/10 17:14:11 Worker 6714 starting map task 2 on file ../pg-frankenstein.txt
2025/01/10 17:14:12 Worker 6715 starting map task 3 on file ../pg-grimm.txt
2025/01/10 17:14:13 Worker 6714 starting map task 4 on file ../pg-huckleberry_finn.txt
2025/01/10 17:14:13 Worker 6715 starting map task 5 on file ../pg-metamorphosis.txt
2025/01/10 17:14:14 Worker 6715 starting map task 6 on file ../pg-sherlock_holmes.txt
2025/01/10 17:14:15 Worker 6714 starting map task 7 on file ../pg-tom_sawyer.txt
2025/01/10 17:14:18 Worker 6714 starting reduce task 0
2025/01/10 17:14:18 Worker 6715 starting reduce task 1
2025/01/10 17:14:19 Worker 6714 starting reduce task 2
2025/01/10 17:14:19 Worker 6715 starting reduce task 3
2025/01/10 17:14:19 Worker 6714 starting reduce task 4
2025/01/10 17:14:20 Worker 6715 starting reduce task 5
2025/01/10 17:14:20 Worker 6714 starting reduce task 6
2025/01/10 17:14:21 Worker 6715 starting reduce task 7
2025/01/10 17:14:21 Worker 6714 starting reduce task 8
2025/01/10 17:14:21 Worker 6715 starting reduce task 9
2025/01/10 17:14:22 Worker 6715 exiting
2025/01/10 17:14:23 Worker 6714 exiting
--- indexer test: PASS
*** Starting map parallelism test.
2025/01/10 17:14:31 Worker 6752 starting map task 1 on file ../pg-dorian_gray.txt
2025/01/10 17:14:31 Worker 6751 starting map task 0 on file ../pg-being_ernest.txt
2025/01/10 17:14:32 Worker 6751 starting map task 2 on file ../pg-frankenstein.txt
2025/01/10 17:14:32 Worker 6752 starting map task 3 on file ../pg-grimm.txt
2025/01/10 17:14:34 Worker 6752 starting map task 4 on file ../pg-huckleberry_finn.txt
2025/01/10 17:14:34 Worker 6751 starting map task 5 on file ../pg-metamorphosis.txt
2025/01/10 17:14:35 Worker 6751 starting map task 6 on file ../pg-sherlock_holmes.txt
2025/01/10 17:14:35 Worker 6752 starting map task 7 on file ../pg-tom_sawyer.txt
2025/01/10 17:14:36 Worker 6751 starting reduce task 0
2025/01/10 17:14:36 Worker 6751 starting reduce task 1
2025/01/10 17:14:36 Worker 6751 starting reduce task 2
2025/01/10 17:14:36 Worker 6751 starting reduce task 3
2025/01/10 17:14:36 Worker 6751 starting reduce task 4
2025/01/10 17:14:36 Worker 6751 starting reduce task 5
2025/01/10 17:14:36 Worker 6751 starting reduce task 6
2025/01/10 17:14:36 Worker 6751 starting reduce task 7
2025/01/10 17:14:36 Worker 6751 starting reduce task 8
2025/01/10 17:14:36 Worker 6751 starting reduce task 9
2025/01/10 17:14:36 Worker 6751 exiting
2025/01/10 17:14:37 Worker 6752 exiting
--- map parallelism test: PASS
*** Starting reduce parallelism test.
2025/01/10 17:14:45 Worker 6787 starting map task 0 on file ../pg-being_ernest.txt
2025/01/10 17:14:45 Worker 6789 starting map task 1 on file ../pg-dorian_gray.txt
2025/01/10 17:14:45 Worker 6789 starting map task 2 on file ../pg-frankenstein.txt
2025/01/10 17:14:45 Worker 6787 starting map task 3 on file ../pg-grimm.txt
2025/01/10 17:14:45 Worker 6789 starting map task 4 on file ../pg-huckleberry_finn.txt
2025/01/10 17:14:45 Worker 6787 starting map task 5 on file ../pg-metamorphosis.txt
2025/01/10 17:14:45 Worker 6789 starting map task 6 on file ../pg-sherlock_holmes.txt
2025/01/10 17:14:45 Worker 6787 starting map task 7 on file ../pg-tom_sawyer.txt
2025/01/10 17:14:45 Worker 6787 starting reduce task 0
2025/01/10 17:14:46 Worker 6789 starting reduce task 1
2025/01/10 17:14:46 Worker 6787 starting reduce task 2
2025/01/10 17:14:47 Worker 6789 starting reduce task 3
2025/01/10 17:14:47 Worker 6787 starting reduce task 4
2025/01/10 17:14:49 Worker 6789 starting reduce task 5
2025/01/10 17:14:49 Worker 6787 starting reduce task 6
2025/01/10 17:14:50 Worker 6789 starting reduce task 7
2025/01/10 17:14:50 Worker 6789 starting reduce task 8
2025/01/10 17:14:50 Worker 6787 starting reduce task 9
2025/01/10 17:14:52 Worker 6787 exiting
2025/01/10 17:14:53 Worker 6789 exiting
--- reduce parallelism test: PASS
*** Starting job count test.
2025/01/10 17:15:00 Worker 6823 starting map task 0 on file ../pg-being_ernest.txt
2025/01/10 17:15:00 Worker 6821 starting map task 1 on file ../pg-dorian_gray.txt
2025/01/10 17:15:02 Worker 6823 starting map task 2 on file ../pg-frankenstein.txt
2025/01/10 17:15:03 Worker 6821 starting map task 3 on file ../pg-grimm.txt
2025/01/10 17:15:07 Worker 6823 starting map task 4 on file ../pg-huckleberry_finn.txt
2025/01/10 17:15:07 Worker 6821 starting map task 5 on file ../pg-metamorphosis.txt
2025/01/10 17:15:11 Worker 6821 starting map task 6 on file ../pg-sherlock_holmes.txt
2025/01/10 17:15:11 Worker 6823 starting map task 7 on file ../pg-tom_sawyer.txt
2025/01/10 17:15:14 Worker 6823 starting reduce task 0
2025/01/10 17:15:14 Worker 6823 starting reduce task 1
2025/01/10 17:15:14 Worker 6823 starting reduce task 2
2025/01/10 17:15:14 Worker 6823 starting reduce task 3
2025/01/10 17:15:14 Worker 6823 starting reduce task 4
2025/01/10 17:15:14 Worker 6823 starting reduce task 5
2025/01/10 17:15:14 Worker 6823 starting reduce task 6
2025/01/10 17:15:14 Worker 6823 starting reduce task 7
2025/01/10 17:15:14 Worker 6823 starting reduce task 8
2025/01/10 17:15:14 Worker 6823 starting reduce task 9
2025/01/10 17:15:14 Worker 6823 exiting
2025/01/10 17:15:14 Worker 6840 exiting
2025/01/10 17:15:14 Worker 6842 exiting
2025/01/10 17:15:15 Worker 6821 exiting
--- job count test: PASS
*** Starting early exit test.
2025/01/10 17:15:24 Worker 6875 starting map task 0 on file ../pg-being_ernest.txt
2025/01/10 17:15:24 Worker 6872 starting map task 1 on file ../pg-dorian_gray.txt
2025/01/10 17:15:24 Worker 6869 starting map task 2 on file ../pg-frankenstein.txt
2025/01/10 17:15:24 Worker 6875 starting map task 3 on file ../pg-grimm.txt
2025/01/10 17:15:24 Worker 6872 starting map task 4 on file ../pg-huckleberry_finn.txt
2025/01/10 17:15:24 Worker 6869 starting map task 5 on file ../pg-metamorphosis.txt
2025/01/10 17:15:25 Worker 6875 starting map task 6 on file ../pg-sherlock_holmes.txt
2025/01/10 17:15:25 Worker 6869 starting map task 7 on file ../pg-tom_sawyer.txt
2025/01/10 17:15:25 Worker 6869 starting reduce task 0
2025/01/10 17:15:25 Worker 6869 starting reduce task 1
2025/01/10 17:15:25 Worker 6869 starting reduce task 2
2025/01/10 17:15:26 Worker 6872 starting reduce task 3
2025/01/10 17:15:26 Worker 6872 starting reduce task 4
2025/01/10 17:15:26 Worker 6872 starting reduce task 5
2025/01/10 17:15:26 Worker 6872 starting reduce task 6
2025/01/10 17:15:26 Worker 6875 starting reduce task 7
2025/01/10 17:15:26 Worker 6875 starting reduce task 8
2025/01/10 17:15:26 Worker 6875 starting reduce task 9
2025/01/10 17:15:29 Worker 6872 exiting
2025/01/10 17:15:29 Worker 6875 exiting
2025/01/10 17:15:29 Worker 6869 exiting
--- early exit test: PASS
*** Starting crash test.
2025/01/10 17:15:38 Worker 6933 starting map task 0 on file ../pg-being_ernest.txt
2025/01/10 17:15:38 Worker 6924 starting map task 2 on file ../pg-frankenstein.txt
2025/01/10 17:15:38 Worker 6931 starting map task 3 on file ../pg-grimm.txt
2025/01/10 17:15:38 Worker 6929 starting map task 1 on file ../pg-dorian_gray.txt
2025/01/10 17:15:38 Worker 6933 starting map task 4 on file ../pg-huckleberry_finn.txt
2025/01/10 17:15:38 Worker 6933 starting map task 5 on file ../pg-metamorphosis.txt
2025/01/10 17:15:39 Worker 6958 starting map task 6 on file ../pg-sherlock_holmes.txt
2025/01/10 17:15:42 Worker 6933 starting map task 7 on file ../pg-tom_sawyer.txt
2025/01/10 17:16:20 Worker 6958 starting map task 1 on file ../pg-dorian_gray.txt
2025/01/10 17:16:20 Worker 6931 starting map task 2 on file ../pg-frankenstein.txt
2025/01/10 17:16:29 Worker 6958 starting reduce task 0
2025/01/10 17:16:29 Worker 6958 starting reduce task 1
2025/01/10 17:16:29 Worker 6958 starting reduce task 2
2025/01/10 17:16:29 Worker 6931 starting reduce task 3
2025/01/10 17:16:29 Worker 6931 starting reduce task 4
2025/01/10 17:16:29 Worker 6931 starting reduce task 5
2025/01/10 17:16:30 Worker 6933 starting reduce task 6
2025/01/10 17:16:30 Worker 6933 starting reduce task 7
2025/01/10 17:16:30 Worker 6933 starting reduce task 8
2025/01/10 17:16:30 Worker 6933 starting reduce task 9
2025/01/10 17:17:10 Worker 6958 starting reduce task 5
2025/01/10 17:17:16 Worker 6958 exiting
2025/01/10 17:17:17 Worker 6973 exiting
2025/01/10 17:17:17 Worker 6933 exiting
2025/01/10 17:17:18 Worker 6995 exiting
2025/01/10 17:17:18 Worker 6987 exiting
2025/01/10 17:17:19 Worker 7011 exiting
2025/01/10 17:17:19 Worker 7018 exiting
2025/01/10 17:17:19 Worker 7004 exiting
2025/01/10 17:17:21 Worker 7028 exiting
2025/01/10 17:17:21 Worker 7031 exiting
2025/01/10 17:17:21 Worker 7032 exiting
2025/01/10 17:17:22 Worker 7056 exiting
2025/01/10 17:17:22 Worker 7053 exiting
2025/01/10 17:17:22 Worker 7054 exiting
--- crash test: PASS
*** PASSED ALL TESTS
```
