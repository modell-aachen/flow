defmodule Ariadne.Flow.Examples.Course.SubscribeStudentToCourseTest do
  use Ariadne.Flow.Test.Gwt, async: true
  alias Ariadne.Flow.Examples.Course.Events
  alias Ariadne.Flow.Examples.Course.SubscribeStudentToCourse

  gwt "Subscribing a student to a course" do
    err("for a non-existing course",
      given: [],
      when: SubscribeStudentToCourse.command(%{course_id: "course-1", student_id: "student-1"}),
      then: :course_not_found
    )

    err("when the course is at full capacity",
      given: [
        %Events.CourseDefined{course_id: "course-1", capacity: 2},
        %Events.StudentSubscribedToCourse{course_id: "course-1", student_id: "student-1"},
        %Events.StudentSubscribedToCourse{course_id: "course-1", student_id: "student-2"}
      ],
      when: SubscribeStudentToCourse.command(%{course_id: "course-1", student_id: "student-3"}),
      then: :course_full
    )

    err("when the student is already subscribed",
      given: [
        %Events.CourseDefined{course_id: "course-1", capacity: 2},
        %Events.StudentSubscribedToCourse{course_id: "course-1", student_id: "student-1"}
      ],
      when: SubscribeStudentToCourse.command(%{course_id: "course-1", student_id: "student-1"}),
      then: :student_already_subscribed_to_course
    )

    err("when the student has reached the subscription limit",
      given: [
        %Events.CourseDefined{course_id: "course-6", capacity: 10},
        %Events.StudentSubscribedToCourse{course_id: "course-1", student_id: "student-1"},
        %Events.StudentSubscribedToCourse{course_id: "course-2", student_id: "student-1"},
        %Events.StudentSubscribedToCourse{course_id: "course-3", student_id: "student-1"},
        %Events.StudentSubscribedToCourse{course_id: "course-4", student_id: "student-1"},
        %Events.StudentSubscribedToCourse{course_id: "course-5", student_id: "student-1"}
      ],
      when: SubscribeStudentToCourse.command(%{course_id: "course-6", student_id: "student-1"}),
      then: :student_subscription_limit_reached
    )

    ok("for an existing course with available capacity",
      given: [
        %Events.CourseDefined{course_id: "course-1", capacity: 2},
        %Events.StudentSubscribedToCourse{course_id: "course-1", student_id: "student-1"}
      ],
      when: SubscribeStudentToCourse.command(%{course_id: "course-1", student_id: "student-2"}),
      then: [%Events.StudentSubscribedToCourse{course_id: "course-1", student_id: "student-2"}]
    )
  end
end
